//go:build integration

// This file holds integration tests that require a live PostgreSQL instance and
// are excluded from normal unit runs. Run them in the end-to-end phase with:
//
//	go test -tags integration ./collector/internal/writer -run Postgres
//
// with TEST_DB_DSN pointing at a database that has the gpu_samples schema loaded.
package writer

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/collector/internal/parser"
)

func TestPostgresInsertIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		t.Skip("set TEST_DB_DSN to run the Postgres integration test")
	}
	ctx := context.Background()

	pg, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()

	s := parser.Sample{
		Timestamp: time.Now().UTC(),
		Metric:    "DCGM_FI_DEV_GPU_UTIL",
		Value:     42,
		UUID:      "GPU-integration-test",
		GPUIndex:  "0",
	}
	if err := pg.Insert(ctx, []parser.Sample{s}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Inserting the same sample again must be a no-op (idempotent upsert).
	if err := pg.Insert(ctx, []parser.Sample{s}); err != nil {
		t.Fatalf("insert (duplicate): %v", err)
	}
}
