package store

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Memory is an in-memory Reader used by unit tests and local runs. It is safe for
// concurrent use.
type Memory struct {
	mu      sync.RWMutex
	samples []stored
}

type stored struct {
	gpu   GPU
	ts    time.Time
	metric string
	value  float64
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory { return &Memory{} }

// Add records a sample for a GPU. Intended for seeding tests and local demos.
func (m *Memory) Add(gpu GPU, s Sample) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.samples = append(m.samples, stored{gpu: gpu, ts: s.Timestamp, metric: s.Metric, value: s.Value})
}

// ListGPUs returns the distinct GPUs (by uuid), sorted by uuid.
func (m *Memory) ListGPUs(_ context.Context) ([]GPU, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	byUUID := make(map[string]GPU)
	for _, s := range m.samples {
		if _, ok := byUUID[s.gpu.UUID]; !ok {
			byUUID[s.gpu.UUID] = s.gpu
		}
	}
	out := make([]GPU, 0, len(byUUID))
	for _, g := range byUUID {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UUID < out[j].UUID })
	return out, nil
}

// Telemetry returns samples for a GPU (by uuid) matching q, ordered by timestamp.
// Time bounds are inclusive; an empty q.Metric matches all metrics.
func (m *Memory) Telemetry(_ context.Context, uuid string, q Query) ([]Sample, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []Sample
	for _, s := range m.samples {
		if s.gpu.UUID != uuid {
			continue
		}
		if q.Metric != "" && s.metric != q.Metric {
			continue
		}
		if !q.Start.IsZero() && s.ts.Before(q.Start) {
			continue
		}
		if !q.End.IsZero() && s.ts.After(q.End) {
			continue
		}
		out = append(out, Sample{Timestamp: s.ts, Metric: s.metric, Value: s.value})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.Before(out[j].Timestamp) })
	return out, nil
}

