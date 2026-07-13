// Package partition implements a single ordered, bounded, in-memory log for one
// topic partition. It assigns monotonically increasing offsets, serves blocking
// and non-blocking reads, and applies producer backpressure: when the retained
// buffer is full, appends block until a consumer's progress lets the broker
// reclaim space via TruncateBelow. This bounds memory usage under load.
package partition

import (
	"sync"
	"time"
)

// Record is a message stored in a partition together with its assigned offset.
type Record struct {
	Offset            int64
	Key               []byte
	Value             []byte
	TimestampUnixNano int64
}

// Partition is a bounded, ordered, in-memory log. It is safe for concurrent use
// by producers (Append) and consumers (Read/Wait), with the broker reclaiming
// space via TruncateBelow. The zero value is not usable; call New.
type Partition struct {
	id     int
	maxLen int

	mu         sync.Mutex
	notFull    *sync.Cond // broadcast when space is reclaimed
	notEmpty   *sync.Cond // broadcast when a record is appended
	records    []Record
	baseOffset int64 // offset of records[0]; equals NextOffset when empty
	closed     bool
}

// New returns a partition with the given id that retains at most maxLen records
// before applying backpressure to producers. A non-positive maxLen defaults to 1024.
func New(id, maxLen int) *Partition {
	if maxLen <= 0 {
		maxLen = 1024
	}
	p := &Partition{id: id, maxLen: maxLen}
	p.notFull = sync.NewCond(&p.mu)
	p.notEmpty = sync.NewCond(&p.mu)
	return p
}

// ID returns the partition id.
func (p *Partition) ID() int { return p.id }

// Append stores a record and returns its assigned offset. If the partition is
// full it blocks (backpressure) until space is reclaimed or the partition is
// closed; it returns -1 if the partition was closed while waiting.
func (p *Partition) Append(key, value []byte) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.records) >= p.maxLen && !p.closed {
		p.notFull.Wait()
	}
	if p.closed {
		return -1
	}
	off := p.baseOffset + int64(len(p.records))
	p.records = append(p.records, Record{
		Offset:            off,
		Key:               key,
		Value:             value,
		TimestampUnixNano: time.Now().UnixNano(),
	})
	p.notEmpty.Broadcast()
	return off
}

// Read returns up to max records with Offset >= from without blocking. If from is
// below the retained window it is clamped to the oldest retained offset. Returns
// nil when nothing is available.
func (p *Partition) Read(from int64, max int) []Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.readLocked(from, max)
}

// Wait blocks until at least one record with Offset >= from is available or the
// partition is closed, then returns up to max such records (nil if closed).
func (p *Partition) Wait(from int64, max int) []Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	for !p.closed {
		if recs := p.readLocked(from, max); len(recs) > 0 {
			return recs
		}
		p.notEmpty.Wait()
	}
	return nil
}

func (p *Partition) readLocked(from int64, max int) []Record {
	if from < p.baseOffset {
		from = p.baseOffset
	}
	start := int(from - p.baseOffset)
	if start < 0 || start >= len(p.records) {
		return nil
	}
	end := len(p.records)
	if max > 0 && start+max < end {
		end = start + max
	}
	out := make([]Record, end-start)
	copy(out, p.records[start:end])
	return out
}

// TruncateBelow drops all records with Offset < offset, reclaiming memory and
// waking producers blocked on backpressure. Offsets already truncated are ignored.
func (p *Partition) TruncateBelow(offset int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset <= p.baseOffset {
		return
	}
	drop := int(offset - p.baseOffset)
	if drop > len(p.records) {
		drop = len(p.records)
	}
	p.records = append(p.records[:0], p.records[drop:]...)
	p.baseOffset += int64(drop)
	p.notFull.Broadcast()
}

// NextOffset returns the offset that the next appended record will receive.
func (p *Partition) NextOffset() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.baseOffset + int64(len(p.records))
}

// BaseOffset returns the offset of the oldest retained record.
func (p *Partition) BaseOffset() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.baseOffset
}

// Len returns the number of retained records.
func (p *Partition) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.records)
}

// Close marks the partition closed and wakes all blocked producers and consumers.
func (p *Partition) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.notEmpty.Broadcast()
	p.notFull.Broadcast()
}
