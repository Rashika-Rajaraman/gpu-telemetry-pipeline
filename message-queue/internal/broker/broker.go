// Package broker ties partitions and consumer groups together into the message
// queue server. It manages topics (each a fixed set of partitions), routes
// produced messages to a partition by key hash (preserving per-key ordering),
// accepts TCP connections, and drives producer and consumer sessions over the
// wire protocol with rebalancing, backpressure, and at-least-once delivery.
package broker

import (
	"bufio"
	"context"
	"hash/fnv"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/gpu-telemetry-pipeline/message-queue/internal/group"
	"github.com/gpu-telemetry-pipeline/message-queue/internal/partition"
	"github.com/gpu-telemetry-pipeline/message-queue/internal/wire"
)

// Config configures a Broker.
type Config struct {
	// Partitions is the number of partitions created per topic. It should exceed
	// the maximum number of consumers so load spreads evenly.
	Partitions int
	// BufferSize is the max records retained per partition before producers block
	// (backpressure), bounding memory.
	BufferSize int
	// BatchSize is the max records delivered in a single frame.
	BatchSize int
	// PollInterval is how often idle delivery workers re-check for new records and
	// assignment changes.
	PollInterval time.Duration
	// Logger receives structured logs; defaults to the logrus standard logger.
	Logger *logrus.Logger
}

func (c Config) withDefaults() Config {
	if c.Partitions <= 0 {
		c.Partitions = 16
	}
	if c.BufferSize <= 0 {
		c.BufferSize = 10000
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 20 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = logrus.StandardLogger()
	}
	return c
}

// Broker is the message queue server. Create it with New and run it with Serve.
type Broker struct {
	cfg    Config
	mu     sync.Mutex
	topics map[string]*topic
}

// New returns a broker with the given configuration (zero fields get defaults).
func New(cfg Config) *Broker {
	return &Broker{cfg: cfg.withDefaults(), topics: make(map[string]*topic)}
}

// log returns the broker's base log entry.
func (b *Broker) log() *logrus.Entry {
	return b.cfg.Logger.WithField("component", "broker")
}

// topic is a named set of partitions plus the consumer groups reading from it.
type topic struct {
	name       string
	partitions []*partition.Partition

	mu        sync.Mutex
	groups    map[string]*group.Group
	rrCounter uint64
}

// topicFor returns the named topic, creating it (with the configured partition
// count) on first use.
func (b *Broker) topicFor(name string) *topic {
	b.mu.Lock()
	defer b.mu.Unlock()
	if t, ok := b.topics[name]; ok {
		return t
	}
	parts := make([]*partition.Partition, b.cfg.Partitions)
	for i := range parts {
		parts[i] = partition.New(i, b.cfg.BufferSize)
	}
	t := &topic{name: name, partitions: parts, groups: make(map[string]*group.Group)}
	b.topics[name] = t
	return t
}

func (t *topic) groupFor(name string) *group.Group {
	t.mu.Lock()
	defer t.mu.Unlock()
	if g, ok := t.groups[name]; ok {
		return g
	}
	g := group.New(name, len(t.partitions))
	t.groups[name] = g
	return g
}

// partitionFor selects the partition for a key. Records sharing a key always land
// on the same partition, preserving their relative order. Empty keys are spread
// round-robin.
func (t *topic) partitionFor(key []byte) int {
	n := len(t.partitions)
	if len(key) == 0 {
		return int(atomic.AddUint64(&t.rrCounter, 1) % uint64(n))
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32() % uint32(n))
}

// minCommitted returns the lowest committed offset across all groups for a
// partition — the point below which records are safe to reclaim. With no groups
// yet, it returns 0 so records are retained (bounded by the buffer's backpressure).
func (t *topic) minCommitted(p int) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.groups) == 0 {
		return 0
	}
	min := int64(-1)
	for _, g := range t.groups {
		c := g.Committed(p)
		if min < 0 || c < min {
			min = c
		}
	}
	if min < 0 {
		return 0
	}
	return min
}

// Serve accepts connections until ctx is cancelled or the listener errors.
func (b *Broker) Serve(ctx context.Context, ln net.Listener) error {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}
		go b.handleConn(ctx, conn)
	}
}

// handleConn reads the first frame to decide whether the peer is a producer
// (Produce) or a consumer (Subscribe), then dispatches accordingly.
func (b *Broker) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	op, payload, err := wire.ReadFrame(r)
	if err != nil {
		return
	}
	switch op {
	case wire.OpProduce:
		b.handleProducer(conn, r, op, payload)
	case wire.OpSubscribe:
		var sub wire.SubscribeReq
		if err := wire.Decode(payload, &sub); err != nil {
			return
		}
		b.handleConsumer(ctx, conn, r, sub)
	default:
		b.log().Warn("unexpected first frame from connection")
		w := bufio.NewWriter(conn)
		_ = wire.WriteFrame(w, wire.OpError, wire.ErrorMsg{Message: "unexpected first frame"})
		_ = w.Flush()
	}
}

// handleProducer processes Produce frames (starting with the already-read first
// frame) and replies with a ProduceAck for each. Append blocks under backpressure,
// which naturally throttles the producer (its ack is withheld until space frees).
func (b *Broker) handleProducer(conn net.Conn, r *bufio.Reader, op wire.Opcode, payload []byte) {
	w := bufio.NewWriter(conn)

	process := func(op wire.Opcode, payload []byte) bool {
		if op != wire.OpProduce {
			_ = wire.WriteFrame(w, wire.OpError, wire.ErrorMsg{Message: "expected produce frame"})
			_ = w.Flush()
			return false
		}
		var req wire.ProduceReq
		if err := wire.Decode(payload, &req); err != nil {
			return false
		}
		t := b.topicFor(req.Topic)
		p := t.partitionFor(req.Key)
		off := t.partitions[p].Append(req.Key, req.Value)
		ack := wire.ProduceAck{Partition: p, Offset: off}
		if off < 0 {
			ack.Error = "partition closed"
		}
		if err := wire.WriteFrame(w, wire.OpProduceAck, ack); err != nil {
			return false
		}
		return w.Flush() == nil
	}

	if !process(op, payload) {
		return
	}
	for {
		op, payload, err := wire.ReadFrame(r)
		if err != nil {
			return
		}
		if !process(op, payload) {
			return
		}
	}
}

// handleConsumer joins the group, then runs a reader goroutine (acks/heartbeats)
// and the delivery manager until the connection drops or ctx is cancelled. On
// exit it leaves the group, which rebalances its partitions onto other members.
func (b *Broker) handleConsumer(ctx context.Context, conn net.Conn, r *bufio.Reader, sub wire.SubscribeReq) {
	t := b.topicFor(sub.Topic)
	g := t.groupFor(sub.Group)
	g.Join(sub.ConsumerID)
	log := b.log().WithFields(logrus.Fields{
		"group":    sub.Group,
		"topic":    sub.Topic,
		"consumer": sub.ConsumerID,
	})
	log.Info("consumer joined group")
	defer func() {
		g.Leave(sub.ConsumerID)
		log.Info("consumer left group")
	}()

	w := bufio.NewWriter(conn)
	var writeMu sync.Mutex
	send := func(op wire.Opcode, payload any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		if err := wire.WriteFrame(w, op, payload); err != nil {
			return err
		}
		return w.Flush()
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Reader goroutine: commit acked offsets (reclaiming partition memory) and
	// answer heartbeats. Any read error ends the session.
	go func() {
		for {
			op, payload, err := wire.ReadFrame(r)
			if err != nil {
				cancel()
				return
			}
			switch op {
			case wire.OpAck:
				var ack wire.AckReq
				if wire.Decode(payload, &ack) == nil {
					for p, off := range ack.Offsets {
						g.Commit(p, off)
						t.partitions[p].TruncateBelow(t.minCommitted(p))
					}
				}
			case wire.OpHeartbeat:
				_ = send(wire.OpHeartbeatAck, wire.Heartbeat{Group: sub.Group, ConsumerID: sub.ConsumerID})
			}
		}
	}()

	b.runDelivery(cctx, t, g, sub.ConsumerID, send)
}

// runDelivery watches this consumer's assignment and starts/stops a delivery
// worker per owned partition as the assignment changes (on rebalance). It pushes
// an AssignMsg whenever the assignment changes.
func (b *Broker) runDelivery(ctx context.Context, t *topic, g *group.Group, consumerID string, send func(wire.Opcode, any) error) {
	workers := make(map[int]context.CancelFunc)
	defer func() {
		for _, cancel := range workers {
			cancel()
		}
	}()

	ticker := time.NewTicker(b.cfg.PollInterval)
	defer ticker.Stop()

	var last []int
	for {
		assign := g.Assignment(consumerID)
		if !equalInts(assign, last) {
			b.log().WithFields(logrus.Fields{"consumer": consumerID, "partitions": assign}).
				Debug("partition assignment changed")
			if err := send(wire.OpAssign, wire.AssignMsg{Partitions: assign}); err != nil {
				return
			}
			last = assign
		}

		owned := make(map[int]bool, len(assign))
		for _, p := range assign {
			owned[p] = true
			if _, running := workers[p]; !running {
				wctx, wcancel := context.WithCancel(ctx)
				workers[p] = wcancel
				go b.deliverPartition(wctx, t, g, p, send)
			}
		}
		for p, cancel := range workers {
			if !owned[p] {
				cancel()
				delete(workers, p)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// deliverPartition streams records for one partition to the consumer, resuming
// from the group's committed offset (so unacked records are redelivered after a
// failover). It advances an in-session cursor as it delivers; the committed offset
// only moves when the consumer acks.
func (b *Broker) deliverPartition(ctx context.Context, t *topic, g *group.Group, p int, send func(wire.Opcode, any) error) {
	part := t.partitions[p]
	cursor := g.Committed(p)
	ticker := time.NewTicker(b.cfg.PollInterval)
	defer ticker.Stop()

	for {
		recs := part.Read(cursor, b.cfg.BatchSize)
		if len(recs) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				continue
			}
		}
		out := make([]wire.Record, len(recs))
		for i, rec := range recs {
			out[i] = wire.Record{
				Topic:             t.name,
				Partition:         p,
				Offset:            rec.Offset,
				Key:               rec.Key,
				Value:             rec.Value,
				TimestampUnixNano: rec.TimestampUnixNano,
			}
		}
		if err := send(wire.OpDeliver, wire.DeliverMsg{Records: out}); err != nil {
			return
		}
		cursor = recs[len(recs)-1].Offset + 1

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
