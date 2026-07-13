// Package writer persists parsed samples. It defines a Store interface (so the
// collector pipeline can be unit-tested with an in-memory fake) plus a PostgreSQL
// implementation using pgx. Writes are batched and idempotent (ON CONFLICT DO
// NOTHING) so that at-least-once delivery from the queue never creates duplicates.
package writer

import (
	"context"
	"fmt"
	"sync"

	"github.com/gpu-telemetry-pipeline/collector/internal/parser"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store persists batches of samples.
type Store interface {
	Insert(ctx context.Context, samples []parser.Sample) error
	Close() error
}

const insertSQL = `
INSERT INTO gpu_samples (ts, metric, value, uuid, gpu_index, device, model_name, hostname)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (uuid, metric, ts) DO NOTHING`

// Postgres is a pgx-backed Store.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres connects to dsn and verifies connectivity.
func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("writer: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("writer: ping: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

// Insert writes all samples in a single batched round-trip, ignoring duplicates.
func (w *Postgres) Insert(ctx context.Context, samples []parser.Sample) error {
	if len(samples) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, s := range samples {
		batch.Queue(insertSQL, s.Timestamp, s.Metric, s.Value, s.UUID, s.GPUIndex, s.Device, s.ModelName, s.Hostname)
	}
	br := w.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range samples {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("writer: insert: %w", err)
		}
	}
	return nil
}

// Close releases the connection pool.
func (w *Postgres) Close() error {
	w.pool.Close()
	return nil
}

// Memory is an in-memory Store used by unit tests and local runs. It deduplicates
// on (uuid, metric, ts) to mirror the database's idempotent upsert.
type Memory struct {
	mu      sync.Mutex
	seen    map[string]bool
	samples []parser.Sample
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{seen: make(map[string]bool)}
}

// Insert appends samples, skipping ones already seen by (uuid, metric, ts).
func (m *Memory) Insert(_ context.Context, samples []parser.Sample) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range samples {
		key := fmt.Sprintf("%s|%s|%d", s.UUID, s.Metric, s.Timestamp.UnixNano())
		if m.seen[key] {
			continue
		}
		m.seen[key] = true
		m.samples = append(m.samples, s)
	}
	return nil
}

// Samples returns a copy of everything stored so far.
func (m *Memory) Samples() []parser.Sample {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]parser.Sample, len(m.samples))
	copy(out, m.samples)
	return out
}

// Close is a no-op for the in-memory store.
func (m *Memory) Close() error { return nil }

var (
	_ Store = (*Postgres)(nil)
	_ Store = (*Memory)(nil)
)

