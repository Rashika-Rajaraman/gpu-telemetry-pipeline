package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres is a PostgreSQL-backed Reader using a pgx connection pool.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres connects to the database at dsn and verifies connectivity.
func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

// Close releases the connection pool.
func (p *Postgres) Close() { p.pool.Close() }

// ListGPUs returns the distinct GPUs (by uuid), sorted by uuid.
func (p *Postgres) ListGPUs(ctx context.Context) ([]GPU, error) {
	const q = `SELECT DISTINCT ON (uuid) uuid, gpu_index, device, model_name, hostname
	           FROM gpu_samples
	           ORDER BY uuid`
	rows, err := p.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: list gpus: %w", err)
	}
	defer rows.Close()

	var out []GPU
	for rows.Next() {
		var g GPU
		if err := rows.Scan(&g.UUID, &g.GPUIndex, &g.Device, &g.ModelName, &g.Hostname); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// Telemetry returns samples for a GPU (by uuid) matching q, ordered by timestamp.
// Time bounds are inclusive; an empty q.Metric matches all metrics. The query is
// built with positional parameters, so all inputs are safely bound (no injection).
func (p *Postgres) Telemetry(ctx context.Context, uuid string, q Query) ([]Sample, error) {
	var sb strings.Builder
	sb.WriteString(`SELECT ts, metric, value FROM gpu_samples WHERE uuid = $1`)
	args := []any{uuid}
	if !q.Start.IsZero() {
		args = append(args, q.Start)
		fmt.Fprintf(&sb, " AND ts >= $%d", len(args))
	}
	if !q.End.IsZero() {
		args = append(args, q.End)
		fmt.Fprintf(&sb, " AND ts <= $%d", len(args))
	}
	if q.Metric != "" {
		args = append(args, q.Metric)
		fmt.Fprintf(&sb, " AND metric = $%d", len(args))
	}
	sb.WriteString(" ORDER BY ts")

	rows, err := p.pool.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("store: telemetry: %w", err)
	}
	defer rows.Close()

	var out []Sample
	for rows.Next() {
		var s Sample
		if err := rows.Scan(&s.Timestamp, &s.Metric, &s.Value); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

var _ Reader = (*Postgres)(nil)
