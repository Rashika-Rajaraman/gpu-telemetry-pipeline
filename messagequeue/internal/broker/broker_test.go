package broker

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/cisco-interview/telemetry-pipeline/messagequeue/internal/wire"
)

func TestPartitionForDeterministicAndCovers(t *testing.T) {
	b := New(Config{Partitions: 8})
	tt := b.topicFor("telemetry")

	// Same key → same partition, every time.
	key := []byte("GPU-abc")
	first := tt.partitionFor(key)
	for i := 0; i < 100; i++ {
		if got := tt.partitionFor(key); got != first {
			t.Fatalf("partitionFor not deterministic: %d vs %d", got, first)
		}
	}

	// Distinct keys should touch more than one partition.
	seen := map[int]bool{}
	for i := 0; i < 200; i++ {
		seen[tt.partitionFor([]byte(fmt.Sprintf("GPU-%d", i)))] = true
	}
	if len(seen) < 2 {
		t.Fatalf("keys mapped to only %d partitions", len(seen))
	}
}

func discardLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

func newTestBroker(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	b := New(Config{
		Partitions:   4,
		BufferSize:   1000,
		BatchSize:    10,
		PollInterval: 5 * time.Millisecond,
		Logger:       discardLogger(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = b.Serve(ctx, ln) }()
	return ln.Addr().String(), func() { cancel(); _ = ln.Close() }
}

func TestEndToEndProduceConsume(t *testing.T) {
	addr, stop := newTestBroker(t)
	defer stop()

	const topicName = "telemetry"
	const n = 12

	// Produce n messages on one connection, reading each ack.
	pconn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial producer: %v", err)
	}
	pw := bufio.NewWriter(pconn)
	pr := bufio.NewReader(pconn)
	want := map[string]bool{}
	for i := 0; i < n; i++ {
		val := fmt.Sprintf("sample-%d", i)
		want[val] = true
		key := []byte(fmt.Sprintf("GPU-%d", i%3))
		if err := wire.WriteFrame(pw, wire.OpProduce, wire.ProduceReq{Topic: topicName, Key: key, Value: []byte(val)}); err != nil {
			t.Fatalf("produce write: %v", err)
		}
		op, payload, err := wire.ReadFrame(pr)
		if err != nil || op != wire.OpProduceAck {
			t.Fatalf("expected produce ack, got op=%v err=%v", op, err)
		}
		var ack wire.ProduceAck
		if err := wire.Decode(payload, &ack); err != nil || ack.Error != "" {
			t.Fatalf("bad ack: %+v err=%v", ack, err)
		}
	}
	_ = pconn.Close()

	// Consume: subscribe and collect all n values, then ack.
	cconn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial consumer: %v", err)
	}
	defer cconn.Close()
	cw := bufio.NewWriter(cconn)
	cr := bufio.NewReader(cconn)
	if err := wire.WriteFrame(cw, wire.OpSubscribe, wire.SubscribeReq{Group: "collectors", Topic: topicName, ConsumerID: "c1"}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	got := map[string]bool{}
	offsets := map[int]int64{}
	deadline := time.Now().Add(3 * time.Second)
	for len(got) < n {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: got %d/%d records", len(got), n)
		}
		_ = cconn.SetReadDeadline(time.Now().Add(2 * time.Second))
		op, payload, err := wire.ReadFrame(cr)
		if err != nil {
			t.Fatalf("consumer read: %v", err)
		}
		switch op {
		case wire.OpAssign:
			// assignment received; nothing to do
		case wire.OpDeliver:
			var d wire.DeliverMsg
			if err := wire.Decode(payload, &d); err != nil {
				t.Fatalf("decode deliver: %v", err)
			}
			for _, rec := range d.Records {
				got[string(rec.Value)] = true
				if rec.Offset+1 > offsets[rec.Partition] {
					offsets[rec.Partition] = rec.Offset + 1
				}
			}
		}
	}

	for v := range want {
		if !got[v] {
			t.Fatalf("missing value %q", v)
		}
	}

	// Ack everything (exercises commit + truncate path).
	if err := wire.WriteFrame(cw, wire.OpAck, wire.AckReq{Group: "collectors", Offsets: offsets}); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

func TestPerKeyOrdering(t *testing.T) {
	addr, stop := newTestBroker(t)
	defer stop()

	const topicName = "telemetry"
	const n = 20
	key := []byte("GPU-fixed")

	pconn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	pw := bufio.NewWriter(pconn)
	pr := bufio.NewReader(pconn)
	for i := 0; i < n; i++ {
		if err := wire.WriteFrame(pw, wire.OpProduce, wire.ProduceReq{Topic: topicName, Key: key, Value: []byte(fmt.Sprintf("%d", i))}); err != nil {
			t.Fatalf("produce: %v", err)
		}
		if _, _, err := wire.ReadFrame(pr); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	_ = pconn.Close()

	cconn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cconn.Close()
	cw := bufio.NewWriter(cconn)
	cr := bufio.NewReader(cconn)
	_ = wire.WriteFrame(cw, wire.OpSubscribe, wire.SubscribeReq{Group: "g", Topic: topicName, ConsumerID: "c1"})

	var seq []string
	deadline := time.Now().Add(3 * time.Second)
	for len(seq) < n {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: got %d/%d", len(seq), n)
		}
		_ = cconn.SetReadDeadline(time.Now().Add(2 * time.Second))
		op, payload, err := wire.ReadFrame(cr)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if op == wire.OpDeliver {
			var d wire.DeliverMsg
			_ = wire.Decode(payload, &d)
			for _, rec := range d.Records {
				seq = append(seq, string(rec.Value))
			}
		}
	}
	// All records share a key → same partition → delivered in produce order.
	for i := 0; i < n; i++ {
		if seq[i] != fmt.Sprintf("%d", i) {
			t.Fatalf("out of order at %d: got %q, seq=%v", i, seq[i], seq)
		}
	}
}

func TestBrokerUnexpectedFirstFrame(t *testing.T) {
	addr, stop := newTestBroker(t)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Send an Ack as the first frame — neither Produce nor Subscribe.
	w := bufio.NewWriter(conn)
	if err := wire.WriteFrame(w, wire.OpAck, wire.AckReq{Group: "g"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	op, _, err := wire.ReadFrame(bufio.NewReader(conn))
	if err != nil || op != wire.OpError {
		t.Fatalf("expected OpError, got op=%v err=%v", op, err)
	}
}

func TestPartitionForEmptyKeyRoundRobin(t *testing.T) {
	b := New(Config{Partitions: 4})
	tt := b.topicFor("t")
	seen := map[int]bool{}
	for i := 0; i < 8; i++ {
		seen[tt.partitionFor(nil)] = true
	}
	if len(seen) < 2 {
		t.Fatalf("empty-key routing hit only %d partitions", len(seen))
	}
}

func TestMinCommittedAcrossGroups(t *testing.T) {
	b := New(Config{Partitions: 2})
	tt := b.topicFor("t")
	g1 := tt.groupFor("a")
	g2 := tt.groupFor("b")
	g1.Commit(0, 10)
	g2.Commit(0, 4)
	// The safe reclaim point is the minimum committed offset across groups.
	if got := tt.minCommitted(0); got != 4 {
		t.Fatalf("minCommitted = %d, want 4", got)
	}
}

func TestProducerSendsUnexpectedFrame(t *testing.T) {
	addr, stop := newTestBroker(t)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	w := bufio.NewWriter(conn)
	r := bufio.NewReader(conn)

	// A valid produce first, to enter the producer session.
	if err := wire.WriteFrame(w, wire.OpProduce, wire.ProduceReq{Topic: "t", Value: []byte("v")}); err != nil {
		t.Fatalf("produce: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if op, _, _ := wire.ReadFrame(r); op != wire.OpProduceAck {
		t.Fatalf("expected produce ack, got %v", op)
	}

	// Now an unexpected Subscribe frame in the producer session -> error.
	if err := wire.WriteFrame(w, wire.OpSubscribe, wire.SubscribeReq{Group: "g", Topic: "t", ConsumerID: "c"}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	op, _, err := wire.ReadFrame(r)
	if err != nil || op != wire.OpError {
		t.Fatalf("expected OpError, got op=%v err=%v", op, err)
	}
}
