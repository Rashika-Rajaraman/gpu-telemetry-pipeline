package store

import (
	"context"
	"testing"
	"time"
)

func ts(sec int) time.Time { return time.Date(2025, 7, 18, 20, 42, sec, 0, time.UTC) }

func seed() *Memory {
	m := NewMemory()
	gpuA := GPU{UUID: "GPU-aaa", GPUIndex: "0", Device: "nvidia0", ModelName: "H100", Hostname: "host-1"}
	gpuB := GPU{UUID: "GPU-bbb", GPUIndex: "1", Device: "nvidia1", ModelName: "H100", Hostname: "host-1"}
	// Intentionally out of time order to verify sorting.
	m.Add(gpuA, Sample{Timestamp: ts(30), Metric: "DCGM_FI_DEV_GPU_UTIL", Value: 50})
	m.Add(gpuA, Sample{Timestamp: ts(10), Metric: "DCGM_FI_DEV_GPU_UTIL", Value: 10})
	m.Add(gpuA, Sample{Timestamp: ts(20), Metric: "DCGM_FI_DEV_POWER_USAGE", Value: 200})
	m.Add(gpuB, Sample{Timestamp: ts(15), Metric: "DCGM_FI_DEV_GPU_UTIL", Value: 99})
	return m
}

func TestListGPUsDistinctSorted(t *testing.T) {
	m := seed()
	gpus, err := m.ListGPUs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(gpus) != 2 {
		t.Fatalf("got %d GPUs, want 2", len(gpus))
	}
	if gpus[0].UUID != "GPU-aaa" || gpus[1].UUID != "GPU-bbb" {
		t.Fatalf("unexpected order: %v", gpus)
	}
	if gpus[0].Hostname != "host-1" || gpus[0].Device != "nvidia0" {
		t.Fatalf("metadata not preserved: %+v", gpus[0])
	}
}

func TestTelemetryOrderedByTime(t *testing.T) {
	m := seed()
	got, err := m.Telemetry(context.Background(), "GPU-aaa", Query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d samples, want 3", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].Timestamp.Before(got[i-1].Timestamp) {
			t.Fatalf("not ordered by time: %v", got)
		}
	}
}

func TestTelemetryTimeWindowInclusive(t *testing.T) {
	m := seed()
	got, err := m.Telemetry(context.Background(), "GPU-aaa", Query{Start: ts(10), End: ts(20)})
	if err != nil {
		t.Fatal(err)
	}
	// ts(10) and ts(20) are inclusive; ts(30) excluded.
	if len(got) != 2 {
		t.Fatalf("got %d, want 2 (%v)", len(got), got)
	}
	if !got[0].Timestamp.Equal(ts(10)) || !got[1].Timestamp.Equal(ts(20)) {
		t.Fatalf("window bounds wrong: %v", got)
	}
}

func TestTelemetryMetricFilter(t *testing.T) {
	m := seed()
	got, err := m.Telemetry(context.Background(), "GPU-aaa", Query{Metric: "DCGM_FI_DEV_POWER_USAGE"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Value != 200 {
		t.Fatalf("metric filter wrong: %v", got)
	}
}

func TestTelemetryUnknownGPU(t *testing.T) {
	m := seed()
	got, err := m.Telemetry(context.Background(), "GPU-nope", Query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no samples, got %d", len(got))
	}
}

// compile-time check that Memory satisfies Reader.
var _ Reader = (*Memory)(nil)
