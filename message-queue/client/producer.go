// Package client is the public SDK for the custom message queue. It is the ONLY
// code shared across components: the streamer imports Producer, the collector
// imports Consumer. It speaks the wire protocol defined in
// message-queue/internal/wire and hides all framing/connection details.
package client

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/gpu-telemetry-pipeline/message-queue/internal/wire"
)

// Producer publishes messages to the broker. It is safe for concurrent use;
// Publish calls are serialized so each request is matched with its ack.
type Producer struct {
	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

// Dial connects a producer to the broker at addr (host:port).
func Dial(addr string) (*Producer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("client: dial producer: %w", err)
	}
	return &Producer{
		conn: conn,
		r:    bufio.NewReader(conn),
		w:    bufio.NewWriter(conn),
	}, nil
}

// Publish appends a message to topic. Messages sharing a key are routed to the
// same partition, preserving their order. It returns the assigned partition and
// offset. Publish blocks while the broker applies backpressure (a full partition),
// which is the intended throttling signal to the caller.
func (p *Producer) Publish(topic string, key, value []byte) (partition int, offset int64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := wire.WriteFrame(p.w, wire.OpProduce, wire.ProduceReq{Topic: topic, Key: key, Value: value}); err != nil {
		return 0, 0, fmt.Errorf("client: publish write: %w", err)
	}
	op, payload, err := wire.ReadFrame(p.r)
	if err != nil {
		return 0, 0, fmt.Errorf("client: publish read ack: %w", err)
	}
	if op != wire.OpProduceAck {
		return 0, 0, fmt.Errorf("client: expected produce ack, got %v", op)
	}
	var ack wire.ProduceAck
	if err := wire.Decode(payload, &ack); err != nil {
		return 0, 0, err
	}
	if ack.Error != "" {
		return ack.Partition, ack.Offset, errors.New(ack.Error)
	}
	return ack.Partition, ack.Offset, nil
}

// Close closes the producer's connection.
func (p *Producer) Close() error {
	return p.conn.Close()
}

