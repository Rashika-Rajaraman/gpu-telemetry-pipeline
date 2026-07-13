// Package store is the read-side persistence abstraction for the API gateway. The
// Reader interface decouples handlers from the backend, enabling an in-memory
// implementation for unit tests and a PostgreSQL (pgx) implementation in
// production.
package store

import (
	"context"
	"time"
)

// GPU identifies a GPU that has telemetry available.
type GPU struct {
	UUID      string `json:"uuid"`
	GPUIndex  string `json:"gpu_index"`
	Device    string `json:"device"`
	ModelName string `json:"model_name"`
	Hostname  string `json:"hostname"`
}

// Sample is a single telemetry reading for a GPU.
type Sample struct {
	Timestamp time.Time `json:"timestamp"`
	Metric    string    `json:"metric"`
	Value     float64   `json:"value"`
}

// Query narrows a telemetry lookup. Zero-value Start/End means unbounded; a
// non-empty Metric restricts results to that metric.
type Query struct {
	Start  time.Time
	End    time.Time
	Metric string
}

// Reader is the read API backing the HTTP handlers.
type Reader interface {
	// ListGPUs returns all GPUs for which telemetry exists, sorted by uuid.
	ListGPUs(ctx context.Context) ([]GPU, error)
	// Telemetry returns the samples for a GPU (by uuid) matching q, ordered by
	// timestamp ascending.
	Telemetry(ctx context.Context, uuid string, q Query) ([]Sample, error)
}

