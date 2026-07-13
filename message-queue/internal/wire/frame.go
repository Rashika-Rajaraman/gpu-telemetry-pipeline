// Package wire defines the message queue's on-the-wire protocol: length-prefixed
// binary frames carrying a one-byte opcode and a JSON payload. It is shared by the
// broker (internal) and the public client SDK, and is the single source of truth
// for how producers, consumers, and the broker talk to each other.
//
// Frame layout (all integers big-endian):
//
//	+---------------------+--------+------------------------+
//	| length (uint32)     | opcode | JSON payload           |
//	| = len(payload) + 1  | 1 byte | length-1 bytes         |
//	+---------------------+--------+------------------------+
//
// The length prefix covers the opcode plus the payload, so a reader knows exactly
// how many bytes to consume for one message.
package wire

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Opcode identifies the kind of message carried by a frame.
type Opcode uint8

const (
	// OpProduce is sent by a producer to append a message to a topic.
	OpProduce Opcode = iota + 1
	// OpProduceAck is the broker's response to OpProduce with the assigned
	// partition and offset (or an error).
	OpProduceAck
	// OpSubscribe is sent by a consumer to join a group and subscribe to a topic.
	OpSubscribe
	// OpAssign is sent by the broker to inform a consumer of its current
	// partition assignment (sent on join and on every rebalance).
	OpAssign
	// OpDeliver carries a batch of records from the broker to a consumer.
	OpDeliver
	// OpAck is sent by a consumer to commit processed offsets per partition.
	OpAck
	// OpHeartbeat is sent periodically by a consumer to keep its membership alive.
	OpHeartbeat
	// OpHeartbeatAck is the broker's response to a heartbeat.
	OpHeartbeatAck
	// OpError carries an error message from the broker to a client.
	OpError
)

// MaxFrameSize caps the size of a single frame to protect the broker and clients
// from malformed or malicious length prefixes (defensive memory management).
const MaxFrameSize = 16 << 20 // 16 MiB

// String renders an opcode for logs and test output.
func (o Opcode) String() string {
	switch o {
	case OpProduce:
		return "Produce"
	case OpProduceAck:
		return "ProduceAck"
	case OpSubscribe:
		return "Subscribe"
	case OpAssign:
		return "Assign"
	case OpDeliver:
		return "Deliver"
	case OpAck:
		return "Ack"
	case OpHeartbeat:
		return "Heartbeat"
	case OpHeartbeatAck:
		return "HeartbeatAck"
	case OpError:
		return "Error"
	default:
		return fmt.Sprintf("Opcode(%d)", uint8(o))
	}
}

// ErrFrameTooLarge is returned when a frame exceeds MaxFrameSize.
var ErrFrameTooLarge = errors.New("wire: frame exceeds max size")

// flusher lets WriteFrame flush buffered writers (e.g. *bufio.Writer) so a frame
// is not left sitting in a buffer.
type flusher interface{ Flush() error }

// WriteFrame marshals payload to JSON and writes a single framed message with the
// given opcode. If w implements Flush() error (e.g. *bufio.Writer), it is flushed.
func WriteFrame(w io.Writer, op Opcode, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("wire: marshal payload: %w", err)
	}
	total := len(body) + 1 // opcode byte + payload
	if total > MaxFrameSize {
		return fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, total)
	}

	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(total))
	hdr[4] = byte(op)
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("wire: write header: %w", err)
	}
	if _, err := w.Write(body); err != nil {
		return fmt.Errorf("wire: write body: %w", err)
	}
	if f, ok := w.(flusher); ok {
		return f.Flush()
	}
	return nil
}

// ReadFrame reads a single framed message and returns its opcode and raw JSON
// payload. It returns io.EOF when the stream ends cleanly between frames.
func ReadFrame(r io.Reader) (Opcode, []byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err // io.EOF / io.ErrUnexpectedEOF propagate to caller
	}
	total := binary.BigEndian.Uint32(hdr[:])
	if total == 0 {
		return 0, nil, errors.New("wire: zero-length frame")
	}
	if total > MaxFrameSize {
		return 0, nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, total)
	}

	buf := make([]byte, total)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, fmt.Errorf("wire: read body: %w", err)
	}
	return Opcode(buf[0]), buf[1:], nil
}

// Decode unmarshals a frame payload into v.
func Decode(payload []byte, v any) error {
	if err := json.Unmarshal(payload, v); err != nil {
		return fmt.Errorf("wire: decode payload: %w", err)
	}
	return nil
}
