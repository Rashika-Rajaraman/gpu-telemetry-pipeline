package wire

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
)

// TestFrameRoundTrip verifies that every payload type survives a WriteFrame →
// ReadFrame → Decode cycle unchanged.
func TestFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		op   Opcode
		in   any
		out  any
	}{
		{
			name: "produce",
			op:   OpProduce,
			in:   ProduceReq{Topic: "telemetry", Key: []byte("GPU-abc"), Value: []byte("row,data,1")},
			out:  &ProduceReq{},
		},
		{
			name: "produce-ack",
			op:   OpProduceAck,
			in:   ProduceAck{Partition: 3, Offset: 42},
			out:  &ProduceAck{},
		},
		{
			name: "subscribe",
			op:   OpSubscribe,
			in:   SubscribeReq{Group: "collectors", Topic: "telemetry", ConsumerID: "c-1"},
			out:  &SubscribeReq{},
		},
		{
			name: "assign",
			op:   OpAssign,
			in:   AssignMsg{Partitions: []int{0, 4, 8, 12}},
			out:  &AssignMsg{},
		},
		{
			name: "deliver",
			op:   OpDeliver,
			in: DeliverMsg{Records: []Record{
				{Topic: "telemetry", Partition: 1, Offset: 7, Key: []byte("k"), Value: []byte("v"), TimestampUnixNano: 123},
			}},
			out: &DeliverMsg{},
		},
		{
			name: "ack",
			op:   OpAck,
			in:   AckReq{Group: "collectors", Offsets: map[int]int64{0: 10, 1: 20}},
			out:  &AckReq{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteFrame(&buf, tc.op, tc.in); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}
			op, payload, err := ReadFrame(&buf)
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if op != tc.op {
				t.Fatalf("opcode = %v, want %v", op, tc.op)
			}
			if err := Decode(payload, tc.out); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			got := reflect.ValueOf(tc.out).Elem().Interface()
			if !reflect.DeepEqual(got, tc.in) {
				t.Fatalf("round-trip mismatch:\n got  %#v\n want %#v", got, tc.in)
			}
		})
	}
}

// TestMultipleFramesSameStream ensures frames are self-delimiting: several frames
// written back-to-back read back in order.
func TestMultipleFramesSameStream(t *testing.T) {
	var buf bytes.Buffer
	want := []Opcode{OpProduce, OpAck, OpHeartbeat}
	for _, op := range want {
		if err := WriteFrame(&buf, op, map[string]int{"n": int(op)}); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	for i, op := range want {
		gotOp, _, err := ReadFrame(&buf)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if gotOp != op {
			t.Fatalf("frame %d opcode = %v, want %v", i, gotOp, op)
		}
	}
}

// TestReadFrameEOF verifies a clean EOF between frames is reported as io.EOF.
func TestReadFrameEOF(t *testing.T) {
	if _, _, err := ReadFrame(bytes.NewReader(nil)); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

// TestReadFrameShortBody verifies a truncated body is a read error, not a panic.
func TestReadFrameShortBody(t *testing.T) {
	// Header claims 10 bytes but only 3 follow.
	b := []byte{0, 0, 0, 10, byte(OpProduce), 1, 2}
	if _, _, err := ReadFrame(bytes.NewReader(b)); err == nil {
		t.Fatal("expected error on short body, got nil")
	}
}

// TestReadFrameZeroLength rejects a zero-length frame.
func TestReadFrameZeroLength(t *testing.T) {
	b := []byte{0, 0, 0, 0}
	if _, _, err := ReadFrame(bytes.NewReader(b)); err == nil {
		t.Fatal("expected error on zero-length frame, got nil")
	}
}

// TestReadFrameTooLarge rejects a length prefix above MaxFrameSize.
func TestReadFrameTooLarge(t *testing.T) {
	var hdr [4]byte
	// MaxFrameSize + 1
	big := uint32(MaxFrameSize + 1)
	hdr[0] = byte(big >> 24)
	hdr[1] = byte(big >> 16)
	hdr[2] = byte(big >> 8)
	hdr[3] = byte(big)
	if _, _, err := ReadFrame(bytes.NewReader(hdr[:])); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestOpcodeString(t *testing.T) {
	cases := map[Opcode]string{
		OpProduce:      "Produce",
		OpProduceAck:   "ProduceAck",
		OpSubscribe:    "Subscribe",
		OpAssign:       "Assign",
		OpDeliver:      "Deliver",
		OpAck:          "Ack",
		OpHeartbeat:    "Heartbeat",
		OpHeartbeatAck: "HeartbeatAck",
		OpError:        "Error",
	}
	for op, want := range cases {
		if got := op.String(); got != want {
			t.Errorf("Opcode(%d).String() = %q, want %q", op, got, want)
		}
	}
	if s := Opcode(200).String(); s == "" {
		t.Error("unknown opcode should have a non-empty string")
	}
}

func TestWriteFrameFlushesBufferedWriter(t *testing.T) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	if err := WriteFrame(w, OpProduce, ProduceReq{Topic: "t", Value: []byte("v")}); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	// Without the internal Flush the frame would still sit in the bufio buffer.
	if buf.Len() == 0 {
		t.Fatal("expected frame flushed to the underlying buffer")
	}
	op, payload, err := ReadFrame(&buf)
	if err != nil || op != OpProduce {
		t.Fatalf("read back: op=%v err=%v", op, err)
	}
	var got ProduceReq
	if err := Decode(payload, &got); err != nil || got.Topic != "t" {
		t.Fatalf("decode: %+v err=%v", got, err)
	}
}

func TestWriteFrameMarshalError(t *testing.T) {
	// A channel cannot be marshaled to JSON.
	if err := WriteFrame(&bytes.Buffer{}, OpProduce, make(chan int)); err == nil {
		t.Fatal("expected marshal error for an unmarshalable payload")
	}
}

func TestDecodeError(t *testing.T) {
	if err := Decode([]byte("not json"), &ProduceReq{}); err == nil {
		t.Fatal("expected decode error for invalid json")
	}
}
