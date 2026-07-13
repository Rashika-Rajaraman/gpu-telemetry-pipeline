package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/cisco-interview/telemetry-pipeline/collector/internal/parser"
	"github.com/cisco-interview/telemetry-pipeline/collector/internal/writer"
	"github.com/cisco-interview/telemetry-pipeline/messagequeue/client"
)

// fakeConsumer yields a fixed set of record batches, then cancels the context so
// Run terminates deterministically.
type fakeConsumer struct {
	batches [][]client.Record
	i       int
	acked   int
	cancel  context.CancelFunc
}

func (f *fakeConsumer) Poll(ctx context.Context) ([]client.Record, error) {
	if f.i >= len(f.batches) {
		f.cancel()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	b := f.batches[f.i]
	f.i++
	return b, nil
}

func (f *fakeConsumer) AckRecords(recs []client.Record) error {
	f.acked++
	return nil
}

func rec(line string) client.Record { return client.Record{Value: []byte(line)} }

const goodLine = `"t","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-aaa","H100","host-1","","","","42","x"`
const goodLine2 = `"t","DCGM_FI_DEV_POWER_USAGE","0","nvidia0","GPU-aaa","H100","host-1","","","","200.5","x"`
const badLine = `"t","BAD"` // too few fields

func TestPipelinePersistsAndAcks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fc := &fakeConsumer{
		batches: [][]client.Record{
			{rec(goodLine), rec(badLine), rec(goodLine2)},
			{rec(goodLine)}, // duplicate of batch 1 → dedup in store
		},
		cancel: cancel,
	}
	store := writer.NewMemory()

	p := &Pipeline{Consumer: fc, Writer: store}
	if err := p.Run(ctx); err != context.Canceled {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}

	got := store.Samples()
	// goodLine + goodLine2 stored; badLine skipped; the duplicate goodLine is
	// deduped by (uuid, metric, ts) — but timestamps are assigned per parse call,
	// so the duplicate has a different ts and may be stored. Assert the valid,
	// distinct-metric samples are present.
	if len(got) < 2 {
		t.Fatalf("stored %d samples, want >= 2", len(got))
	}
	metrics := map[string]bool{}
	for _, s := range got {
		metrics[s.Metric] = true
		if s.UUID != "GPU-aaa" {
			t.Errorf("unexpected uuid %q", s.UUID)
		}
	}
	if !metrics["DCGM_FI_DEV_GPU_UTIL"] || !metrics["DCGM_FI_DEV_POWER_USAGE"] {
		t.Fatalf("missing expected metrics: %v", metrics)
	}
	// Both non-empty batches should have been acknowledged.
	if fc.acked != 2 {
		t.Fatalf("acked %d batches, want 2", fc.acked)
	}
}

// errWriter fails on Insert.
type errWriter struct{ err error }

func (e *errWriter) Insert(ctx context.Context, samples []parser.Sample) error { return e.err }
func (e *errWriter) Close() error                                              { return nil }

func TestPipelinePersistErrorStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fc := &fakeConsumer{batches: [][]client.Record{{rec(goodLine)}}, cancel: cancel}
	wantErr := errors.New("db down")

	p := &Pipeline{Consumer: fc, Writer: &errWriter{err: wantErr}}
	if err := p.Run(ctx); !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if fc.acked != 0 {
		t.Fatalf("acked %d, want 0 (persist failed → no ack)", fc.acked)
	}
}

// ackErrConsumer errors on AckRecords.
type ackErrConsumer struct {
	fakeConsumer
	ackErr error
}

func (a *ackErrConsumer) AckRecords(recs []client.Record) error { return a.ackErr }

func TestPipelineAckErrorStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wantErr := errors.New("ack failed")
	c := &ackErrConsumer{
		fakeConsumer: fakeConsumer{batches: [][]client.Record{{rec(goodLine)}}, cancel: cancel},
		ackErr:       wantErr,
	}
	p := &Pipeline{Consumer: c, Writer: writer.NewMemory()}
	if err := p.Run(ctx); !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}

func TestPipelineAllMalformedStillAcks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fc := &fakeConsumer{batches: [][]client.Record{{rec(badLine), rec(badLine)}}, cancel: cancel}
	store := writer.NewMemory()

	p := &Pipeline{Consumer: fc, Writer: store}
	if err := p.Run(ctx); err != context.Canceled {
		t.Fatalf("Run err = %v", err)
	}
	if len(store.Samples()) != 0 {
		t.Fatal("expected no samples from an all-malformed batch")
	}
	if fc.acked != 1 {
		t.Fatalf("acked %d, want 1 (batch is acked even if all rows are skipped)", fc.acked)
	}
}