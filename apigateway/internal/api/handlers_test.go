package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gpu-telemetry-pipeline/apigateway/internal/store"
)

func ts(sec int) time.Time { return time.Date(2025, 7, 18, 20, 42, sec, 0, time.UTC) }

func newServer() http.Handler {
	m := store.NewMemory()
	gpuA := store.GPU{UUID: "GPU-aaa", GPUIndex: "0", Device: "nvidia0", ModelName: "H100", Hostname: "host-1"}
	gpuB := store.GPU{UUID: "GPU-bbb", GPUIndex: "1", Device: "nvidia1", ModelName: "H100", Hostname: "host-1"}
	m.Add(gpuA, store.Sample{Timestamp: ts(30), Metric: "DCGM_FI_DEV_GPU_UTIL", Value: 50})
	m.Add(gpuA, store.Sample{Timestamp: ts(10), Metric: "DCGM_FI_DEV_GPU_UTIL", Value: 10})
	m.Add(gpuA, store.Sample{Timestamp: ts(20), Metric: "DCGM_FI_DEV_POWER_USAGE", Value: 200})
	m.Add(gpuB, store.Sample{Timestamp: ts(15), Metric: "DCGM_FI_DEV_GPU_UTIL", Value: 99})
	return New(m, nil).Routes()
}

func do(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestListGPUs(t *testing.T) {
	rec := do(t, newServer(), "/api/v1/gpus")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body struct {
		GPUs  []store.GPU `json:"gpus"`
		Count int         `json:"count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Count != 2 || len(body.GPUs) != 2 {
		t.Fatalf("count = %d, want 2", body.Count)
	}
	if body.GPUs[0].UUID != "GPU-aaa" {
		t.Fatalf("first uuid = %q", body.GPUs[0].UUID)
	}
}

func TestTelemetryOrderedByTime(t *testing.T) {
	rec := do(t, newServer(), "/api/v1/gpus/GPU-aaa/telemetry")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body struct {
		Samples []store.Sample `json:"samples"`
		Count   int            `json:"count"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Count != 3 {
		t.Fatalf("count = %d, want 3", body.Count)
	}
	for i := 1; i < len(body.Samples); i++ {
		if body.Samples[i].Timestamp.Before(body.Samples[i-1].Timestamp) {
			t.Fatalf("not ordered by time")
		}
	}
}

func TestTelemetryTimeWindow(t *testing.T) {
	rec := do(t, newServer(), "/api/v1/gpus/GPU-aaa/telemetry?start_time=2025-07-18T20:42:10Z&end_time=2025-07-18T20:42:20Z")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body struct {
		Count int `json:"count"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Count != 2 {
		t.Fatalf("count = %d, want 2 (inclusive window)", body.Count)
	}
}

func TestTelemetryMetricFilter(t *testing.T) {
	rec := do(t, newServer(), "/api/v1/gpus/GPU-aaa/telemetry?metric=DCGM_FI_DEV_POWER_USAGE")
	var body struct {
		Count int `json:"count"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Count != 1 {
		t.Fatalf("count = %d, want 1", body.Count)
	}
}

func TestTelemetryUnknownGPU404(t *testing.T) {
	rec := do(t, newServer(), "/api/v1/gpus/GPU-nope/telemetry")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestTelemetryBadTime400(t *testing.T) {
	rec := do(t, newServer(), "/api/v1/gpus/GPU-aaa/telemetry?start_time=not-a-time")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestHealthAndMetrics(t *testing.T) {
	h := newServer()
	if rec := do(t, h, "/healthz"); rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d", rec.Code)
	}
	// generate some traffic, then scrape metrics
	do(t, h, "/api/v1/gpus")
	rec := do(t, h, "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "apigateway_http_requests_total") {
		t.Fatalf("metrics missing counter:\n%s", rec.Body.String())
	}
}

// errStore fails on every read.
type errStore struct{}

func (errStore) ListGPUs(context.Context) ([]store.GPU, error) {
	return nil, errors.New("store down")
}
func (errStore) Telemetry(context.Context, string, store.Query) ([]store.Sample, error) {
	return nil, errors.New("store down")
}

// failOpenStore returns no telemetry but errors on the existence check.
type failOpenStore struct{}

func (failOpenStore) ListGPUs(context.Context) ([]store.GPU, error) {
	return nil, errors.New("store down")
}
func (failOpenStore) Telemetry(context.Context, string, store.Query) ([]store.Sample, error) {
	return nil, nil
}

func TestListGPUsStoreError(t *testing.T) {
	h := New(errStore{}, nil).Routes()
	if rec := do(t, h, "/api/v1/gpus"); rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestTelemetryStoreError(t *testing.T) {
	h := New(errStore{}, nil).Routes()
	if rec := do(t, h, "/api/v1/gpus/GPU-x/telemetry"); rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestTelemetryEndBeforeStart(t *testing.T) {
	rec := do(t, newServer(), "/api/v1/gpus/GPU-aaa/telemetry?start_time=2025-07-18T20:42:30Z&end_time=2025-07-18T20:42:10Z")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestTelemetryBadEndTime400(t *testing.T) {
	rec := do(t, newServer(), "/api/v1/gpus/GPU-aaa/telemetry?end_time=not-a-time")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestTelemetryFailOpenOnExistenceError(t *testing.T) {
	// Telemetry is empty and the existence check errors, so the handler fails open
	// (returns 200 with no samples) rather than a false 404.
	h := New(failOpenStore{}, nil).Routes()
	if rec := do(t, h, "/api/v1/gpus/GPU-x/telemetry"); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (fail open)", rec.Code)
	}
}

func TestOpenAPISpecEndpoint(t *testing.T) {
	rec := do(t, newServer(), "/openapi.yaml")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/yaml" {
		t.Fatalf("content-type = %q, want application/yaml", ct)
	}
	if !strings.Contains(rec.Body.String(), "openapi:") {
		t.Fatalf("body is not an OpenAPI document:\n%s", rec.Body.String())
	}
}

func TestSwaggerUIEndpoint(t *testing.T) {
	rec := do(t, newServer(), "/docs")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "swagger-ui") {
		t.Fatalf("body is not the Swagger UI page:\n%s", rec.Body.String())
	}
}
