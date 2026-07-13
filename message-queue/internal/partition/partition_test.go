package partition

import (
	"sync"
	"testing"
	"time"
)

func TestAppendAssignsMonotonicOffsets(t *testing.T) {
	p := New(0, 100)
	for i := int64(0); i < 5; i++ {
		if got := p.Append(nil, []byte{byte(i)}); got != i {
			t.Fatalf("Append #%d offset = %d, want %d", i, got, i)
		}
	}
	if p.NextOffset() != 5 {
		t.Fatalf("NextOffset = %d, want 5", p.NextOffset())
	}
}

func TestReadRanges(t *testing.T) {
	p := New(0, 100)
	for i := 0; i < 10; i++ {
		p.Append(nil, []byte{byte(i)})
	}
	recs := p.Read(3, 4)
	if len(recs) != 4 {
		t.Fatalf("len = %d, want 4", len(recs))
	}
	if recs[0].Offset != 3 || recs[3].Offset != 6 {
		t.Fatalf("offsets = %d..%d, want 3..6", recs[0].Offset, recs[3].Offset)
	}
	// max<=0 returns all remaining.
	if all := p.Read(8, 0); len(all) != 2 {
		t.Fatalf("tail len = %d, want 2", len(all))
	}
	// reading past the end returns nil.
	if p.Read(10, 5) != nil {
		t.Fatal("expected nil past end")
	}
}

func TestTruncateBelowReclaimsAndClamps(t *testing.T) {
	p := New(0, 100)
	for i := 0; i < 10; i++ {
		p.Append(nil, nil)
	}
	p.TruncateBelow(4)
	if p.BaseOffset() != 4 {
		t.Fatalf("BaseOffset = %d, want 4", p.BaseOffset())
	}
	if p.Len() != 6 {
		t.Fatalf("Len = %d, want 6", p.Len())
	}
	// Reading below the window clamps up to base offset.
	recs := p.Read(0, 3)
	if len(recs) == 0 || recs[0].Offset != 4 {
		t.Fatalf("expected clamp to offset 4, got %+v", recs)
	}
	// Truncating below the base is a no-op.
	p.TruncateBelow(2)
	if p.BaseOffset() != 4 {
		t.Fatalf("BaseOffset changed to %d", p.BaseOffset())
	}
}

func TestBackpressureBlocksUntilTruncate(t *testing.T) {
	p := New(0, 3) // tiny buffer
	for i := 0; i < 3; i++ {
		p.Append(nil, nil) // fills the buffer
	}

	done := make(chan int64, 1)
	go func() { done <- p.Append(nil, []byte("blocked")) }()

	select {
	case <-done:
		t.Fatal("Append should block while the partition is full")
	case <-time.After(50 * time.Millisecond):
		// expected: still blocked
	}

	p.TruncateBelow(1) // free one slot

	select {
	case off := <-done:
		if off != 3 {
			t.Fatalf("unblocked Append offset = %d, want 3", off)
		}
	case <-time.After(time.Second):
		t.Fatal("Append did not unblock after TruncateBelow")
	}
}

func TestWaitBlocksThenReturns(t *testing.T) {
	p := New(0, 100)
	got := make(chan []Record, 1)
	go func() { got <- p.Wait(0, 10) }()

	select {
	case <-got:
		t.Fatal("Wait should block until a record is appended")
	case <-time.After(50 * time.Millisecond):
	}

	p.Append([]byte("k"), []byte("v"))

	select {
	case recs := <-got:
		if len(recs) != 1 || string(recs[0].Value) != "v" {
			t.Fatalf("Wait returned %+v", recs)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after Append")
	}
}

func TestCloseUnblocksWaiters(t *testing.T) {
	p := New(0, 1)
	p.Append(nil, nil) // full

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); p.Wait(5, 10) }()      // blocked: nothing at offset 5
	go func() { defer wg.Done(); p.Append(nil, nil) }() // blocked: full

	time.Sleep(20 * time.Millisecond)
	p.Close()

	waited := make(chan struct{})
	go func() { wg.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock waiters")
	}
}
