//go:build integration

// This file holds integration tests that require a live PostgreSQL instance and
// are excluded from normal unit runs. Run them in the end-to-end phase with:
//
//	go test -tags integration ./apigateway/internal/store -run Postgres
//
// with TEST_DB_DSN pointing at a database that has the gpu_samples schema loaded.
package store

import (
	"context"
	"os"
	"testing"
)

func TestPostgresReadIntegration(t *testing.T) {
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

	gpus, err := pg.ListGPUs(ctx)
	if err != nil {
		t.Fatalf("list gpus: %v", err)
	}
	if len(gpus) == 0 {
		t.Skip("no gpu_samples rows present; load sample data to exercise reads")
	}

	// Exercise the query builder branches (uuid + time bounds + metric filter).
	uuid := gpus[0].UUID
	if _, err := pg.Telemetry(ctx, uuid, Query{}); err != nil {
		t.Fatalf("telemetry (unbounded): %v", err)
	}
	if _, err := pg.Telemetry(ctx, uuid, Query{Metric: "DCGM_FI_DEV_GPU_UTIL"}); err != nil {
		t.Fatalf("telemetry (metric filter): %v", err)
	}
}
