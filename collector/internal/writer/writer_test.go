package writer

import (
	"context"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/collector/internal/parser"
)

func sample(uuid, metric string, sec int, v float64) parser.Sample {
	return parser.Sample{
		Timestamp: time.Date(2026, 1, 1, 0, 0, sec, 0, time.UTC),
		Metric:    metric,
		Value:     v,
		UUID:      uuid,
	}
}

func TestMemoryInsertAndSamples(t *testing.T) {
	m := NewMemory()
	in := []parser.Sample{
		sample("GPU-a", "UTIL", 1, 10),
		sample("GPU-a", "POWER", 1, 200),
	}
	if err := m.Insert(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if got := m.Samples(); len(got) != 2 {
		t.Fatalf("stored %d, want 2", len(got))
	}
}

func TestMemoryIdempotent(t *testing.T) {
	m := NewMemory()
	s := sample("GPU-a", "UTIL", 5, 10)
	// Insert the same (uuid, metric, ts) twice — mirrors at-least-once redelivery.
	_ = m.Insert(context.Background(), []parser.Sample{s})
	_ = m.Insert(context.Background(), []parser.Sample{s})
	if got := m.Samples(); len(got) != 1 {
		t.Fatalf("stored %d, want 1 (dedup)", len(got))
	}
}

func TestMemoryEmptyInsert(t *testing.T) {
	m := NewMemory()
	if err := m.Insert(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if len(m.Samples()) != 0 {
		t.Fatal("expected empty")
	}
}
