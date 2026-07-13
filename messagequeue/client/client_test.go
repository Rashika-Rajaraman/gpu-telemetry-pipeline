package client

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/cisco-interview/telemetry-pipeline/messagequeue/internal/broker"
	"github.com/cisco-interview/telemetry-pipeline/messagequeue/internal/wire"
)

// startBroker runs an in-process broker on a random port for tests.
func startBroker(t *testing.T, partitions int) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	b := broker.New(broker.Config{
		Partitions:   partitions,
		BufferSize:   1000,
		BatchSize:    16,
		PollInterval: 5 * time.Millisecond,
		Logger:       logger,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = b.Serve(ctx, ln) }()
	return ln.Addr().String(), func() { cancel(); _ = ln.Close() }
}

func TestProducerConsumerRoundTrip(t *testing.T) {
	addr, stop := startBroker(t, 4)
	defer stop()

	prod, err := Dial(addr)
	if err != nil {
		t.Fatalf("dial producer: %v", err)
	}
	defer prod.Close()

	const n = 30
	want := map[string]bool{}
	for i := 0; i < n; i++ {
		v := fmt.Sprintf("sample-%d", i)
		want[v] = true
		if _, _, err := prod.Publish("telemetry", []byte(fmt.Sprintf("GPU-%d", i%5)), []byte(v)); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	cons, err := Subscribe(addr, "collectors", "telemetry", "c1")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cons.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got := map[string]bool{}
	for len(got) < n {
		recs, err := cons.Poll(ctx)
		if err != nil {
			t.Fatalf("poll: %v (got %d/%d)", err, len(got), n)
		}
		for _, r := range recs {
			got[string(r.Value)] = true
		}
		if err := cons.AckRecords(recs); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	for v := range want {
		if !got[v] {
			t.Fatalf("missing %q", v)
		}
	}
}

// TestConsumerGroupSharesLoad verifies that two consumers in one group together
// receive every message exactly once across the partition set (competing
// consumers). Both consumers subscribe and the group rebalances to a stable split
// BEFORE any message is published, so each partition's records can only reach its
// single owner — making both "exactly once" and "load is shared" deterministic.
func TestConsumerGroupSharesLoad(t *testing.T) {
	addr, stop := startBroker(t, 8)
	defer stop()

	c1, err := Subscribe(addr, "collectors", "telemetry", "c1")
	if err != nil {
		t.Fatalf("subscribe c1: %v", err)
	}
	defer c1.Close()
	c2, err := Subscribe(addr, "collectors", "telemetry", "c2")
	if err != nil {
		t.Fatalf("subscribe c2: %v", err)
	}
	defer c2.Close()

	// Let both members join and the group settle into a 4/4 split before producing.
	time.Sleep(300 * time.Millisecond)

	prod, err := Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer prod.Close()

	const n = 80
	for i := 0; i < n; i++ {
		if _, _, err := prod.Publish("telemetry", []byte(fmt.Sprintf("GPU-%d", i)), []byte(fmt.Sprintf("v-%d", i))); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	var mu sync.Mutex
	seen := map[string]int{}
	counts := map[string]int{}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	consume := func(id string, c *Consumer) {
		defer wg.Done()
		for {
			recs, err := c.Poll(ctx)
			if err != nil {
				return
			}
			mu.Lock()
			for _, r := range recs {
				seen[string(r.Value)]++
				counts[id]++
			}
			done := len(seen) >= n
			mu.Unlock()
			_ = c.AckRecords(recs)
			if done {
				cancel()
				return
			}
		}
	}

	wg.Add(2)
	go consume("c1", c1)
	go consume("c2", c2)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Fatalf("saw %d distinct messages, want %d", len(seen), n)
	}
	for v, c := range seen {
		if c < 1 {
			t.Fatalf("message %q seen %d times", v, c)
		}
	}
	// With a stable 4/4 split established before publishing, reaching all n messages
	// necessarily required both consumers to drain their own partitions.
	if counts["c1"] == 0 || counts["c2"] == 0 {
		t.Fatalf("load not shared: c1=%d c2=%d", counts["c1"], counts["c2"])
	}
}

func TestDialError(t *testing.T) {
	if _, err := Dial("127.0.0.1:1"); err == nil {
		t.Fatal("expected error dialing a refused port")
	}
}

func TestSubscribeError(t *testing.T) {
	if _, err := Subscribe("127.0.0.1:1", "g", "t", "c1"); err == nil {
		t.Fatal("expected error subscribing to a refused port")
	}
}

func TestProducerClose(t *testing.T) {
	addr, stop := startBroker(t, 2)
	defer stop()
	p, err := Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestPublishAfterCloseFails(t *testing.T) {
	addr, stop := startBroker(t, 2)
	defer stop()
	p, err := Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = p.Close()
	if _, _, err := p.Publish("t", []byte("k"), []byte("v")); err == nil {
		t.Fatal("expected publish to fail on a closed producer")
	}
}

func TestConsumerCloseIdempotent(t *testing.T) {
	addr, stop := startBroker(t, 2)
	defer stop()
	c, err := Subscribe(addr, "g", "t", "c1")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := c.Close(); err != nil { // second close must be safe
		t.Fatalf("second close: %v", err)
	}
}

func TestAckRecordsEmptyIsNoop(t *testing.T) {
	addr, stop := startBroker(t, 2)
	defer stop()
	c, err := Subscribe(addr, "g", "t", "c1")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer c.Close()
	if err := c.AckRecords(nil); err != nil {
		t.Fatalf("AckRecords(nil): %v", err)
	}
}

func TestAckDirect(t *testing.T) {
	addr, stop := startBroker(t, 2)
	defer stop()
	c, err := Subscribe(addr, "collectors", "t", "c1")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer c.Close()
	if err := c.Ack(map[int]int64{0: 5}); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

func TestPollReturnsAfterClose(t *testing.T) {
	addr, stop := startBroker(t, 2)
	defer stop()
	c, err := Subscribe(addr, "g", "t", "c1")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_, _ = c.Poll(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Poll blocked after Close")
	}
}

func TestHeartbeat(t *testing.T) {
	old := defaultHeartbeatInterval
	defaultHeartbeatInterval = 10 * time.Millisecond
	defer func() { defaultHeartbeatInterval = old }()

	addr, stop := startBroker(t, 2)
	defer stop()
	c, err := Subscribe(addr, "g", "t", "c1")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer c.Close()

	// Allow several heartbeat cycles to fire and be acknowledged.
	time.Sleep(60 * time.Millisecond)
	if err := c.Err(); err != nil {
		t.Fatalf("consumer errored during heartbeats: %v", err)
	}
}

// fakeServer accepts one connection and runs handle on it, letting tests inject
// broker responses the real broker would never send.
func fakeServer(t *testing.T, handle func(net.Conn)) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		handle(conn)
	}()
	return ln.Addr().String(), func() { _ = ln.Close() }
}

func TestPublishBrokerReturnsErrorFrame(t *testing.T) {
	addr, stop := fakeServer(t, func(conn net.Conn) {
		defer conn.Close()
		if _, _, err := wire.ReadFrame(bufio.NewReader(conn)); err != nil {
			return
		}
		w := bufio.NewWriter(conn)
		_ = wire.WriteFrame(w, wire.OpError, wire.ErrorMsg{Message: "boom"})
		_ = w.Flush()
	})
	defer stop()

	p, err := Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer p.Close()
	if _, _, err := p.Publish("t", nil, []byte("v")); err == nil {
		t.Fatal("expected error when broker replies with a non-ack frame")
	}
}

func TestPublishAckWithError(t *testing.T) {
	addr, stop := fakeServer(t, func(conn net.Conn) {
		defer conn.Close()
		if _, _, err := wire.ReadFrame(bufio.NewReader(conn)); err != nil {
			return
		}
		w := bufio.NewWriter(conn)
		_ = wire.WriteFrame(w, wire.OpProduceAck, wire.ProduceAck{Error: "partition closed"})
		_ = w.Flush()
	})
	defer stop()

	p, err := Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer p.Close()
	if _, _, err := p.Publish("t", nil, []byte("v")); err == nil {
		t.Fatal("expected error when the ack carries an error")
	}
}

func TestConsumerReceivesBrokerError(t *testing.T) {
	addr, stop := fakeServer(t, func(conn net.Conn) {
		defer conn.Close()
		if _, _, err := wire.ReadFrame(bufio.NewReader(conn)); err != nil {
			return // subscribe frame
		}
		w := bufio.NewWriter(conn)
		_ = wire.WriteFrame(w, wire.OpError, wire.ErrorMsg{Message: "nope"})
		_ = w.Flush()
		time.Sleep(100 * time.Millisecond) // keep conn open so the client reads it
	})
	defer stop()

	c, err := Subscribe(addr, "g", "t", "c1")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.Poll(ctx); err == nil {
		t.Fatal("expected error after broker error frame")
	}
}
