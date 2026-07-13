package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cisco-interview/telemetry-pipeline/messagequeue/internal/wire"
)

// Record is a message delivered to a consumer.
type Record struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	// TimestampUnixNano is the broker-side append time.
	TimestampUnixNano int64
}

// defaultHeartbeatInterval is how often the consumer proves liveness to the broker.
// It is a var (not const) so tests can shorten it.
var defaultHeartbeatInterval = 5 * time.Second

// Consumer subscribes to a topic as part of a group and receives records for the
// partitions the broker assigns to it. A background goroutine reads deliveries and
// answers heartbeats; callers pull batches with Poll and acknowledge with Ack.
type Consumer struct {
	conn  net.Conn
	r     *bufio.Reader
	w     *bufio.Writer
	group string
	id    string

	writeMu sync.Mutex // serializes writes (Ack + heartbeat)
	recCh   chan []Record

	closeOnce sync.Once
	done      chan struct{}

	errMu sync.Mutex
	err   error
}

// Subscribe connects to the broker at addr and joins group as consumerID,
// subscribing to topic. The broker assigns partitions and begins delivering.
func Subscribe(addr, group, topic, consumerID string) (*Consumer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("client: dial consumer: %w", err)
	}
	c := &Consumer{
		conn:  conn,
		r:     bufio.NewReader(conn),
		w:     bufio.NewWriter(conn),
		group: group,
		id:    consumerID,
		recCh: make(chan []Record, 256),
		done:  make(chan struct{}),
	}
	if err := wire.WriteFrame(c.w, wire.OpSubscribe, wire.SubscribeReq{
		Group:      group,
		Topic:      topic,
		ConsumerID: consumerID,
	}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("client: subscribe: %w", err)
	}
	go c.readLoop()
	go c.heartbeatLoop(defaultHeartbeatInterval)
	return c, nil
}

// readLoop reads frames from the broker until the connection closes, routing
// deliveries to recCh and recording terminal errors.
func (c *Consumer) readLoop() {
	for {
		op, payload, err := wire.ReadFrame(c.r)
		if err != nil {
			c.fail(err)
			return
		}
		switch op {
		case wire.OpDeliver:
			var d wire.DeliverMsg
			if err := wire.Decode(payload, &d); err != nil {
				c.fail(err)
				return
			}
			recs := make([]Record, len(d.Records))
			for i, r := range d.Records {
				recs[i] = Record{
					Topic:             r.Topic,
					Partition:         r.Partition,
					Offset:            r.Offset,
					Key:               r.Key,
					Value:             r.Value,
					TimestampUnixNano: r.TimestampUnixNano,
				}
			}
			select {
			case c.recCh <- recs:
			case <-c.done:
				return
			}
		case wire.OpAssign:
			// Assignment changes are transparent to callers; ignored here.
		case wire.OpHeartbeatAck:
			// liveness confirmed
		case wire.OpError:
			var e wire.ErrorMsg
			_ = wire.Decode(payload, &e)
			c.fail(errors.New("broker: " + e.Message))
			return
		}
	}
}

func (c *Consumer) heartbeatLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
			c.writeMu.Lock()
			err := wire.WriteFrame(c.w, wire.OpHeartbeat, wire.Heartbeat{Group: c.group, ConsumerID: c.id})
			c.writeMu.Unlock()
			if err != nil {
				c.fail(err)
				return
			}
		}
	}
}

// Poll returns the next batch of records, blocking until records arrive, ctx is
// cancelled, or the consumer is closed/failed.
func (c *Consumer) Poll(ctx context.Context) ([]Record, error) {
	select {
	case recs := <-c.recCh:
		return recs, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.Err()
	}
}

// Ack commits per-partition offsets. Each value is the next offset the consumer
// expects (i.e. everything below it is acknowledged). Use AckRecords for a batch
// of delivered records.
func (c *Consumer) Ack(offsets map[int]int64) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return wire.WriteFrame(c.w, wire.OpAck, wire.AckReq{Group: c.group, Offsets: offsets})
}

// AckRecords acknowledges a batch by committing, for each partition, the highest
// delivered offset + 1.
func (c *Consumer) AckRecords(recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	offsets := make(map[int]int64, 4)
	for _, r := range recs {
		if next := r.Offset + 1; next > offsets[r.Partition] {
			offsets[r.Partition] = next
		}
	}
	return c.Ack(offsets)
}

// Err returns the terminal error that ended the consumer, if any.
func (c *Consumer) Err() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.err
}

func (c *Consumer) fail(err error) {
	c.errMu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.errMu.Unlock()
	c.close()
}

// Close stops the consumer and closes its connection. Safe to call multiple times.
func (c *Consumer) Close() error {
	c.close()
	return nil
}

func (c *Consumer) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

