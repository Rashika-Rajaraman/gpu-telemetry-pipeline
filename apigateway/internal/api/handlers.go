// Package api implements the REST handlers for the telemetry service using the
// standard library net/http router (Go 1.22 pattern matching):
//
//	GET /api/v1/gpus                       - list all GPUs
//	GET /api/v1/gpus/{id}/telemetry        - samples for a GPU, ordered by time
//	    optional query: start_time, end_time (RFC3339, inclusive), metric
//	GET /healthz, /readyz                  - probes
//	GET /metrics                           - Prometheus text metrics
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/gpu-telemetry-pipeline/apigateway/internal/store"
)

// Handler serves the telemetry REST API backed by a store.Reader.
type Handler struct {
	store   store.Reader
	logger  *logrus.Logger
	metrics *metrics
}

// New returns a Handler backed by the given store.
func New(s store.Reader, logger *logrus.Logger) *Handler {
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	return &Handler{store: s, logger: logger, metrics: newMetrics()}
}

// Routes builds the HTTP handler with all routes and the metrics middleware.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/gpus", h.listGPUs)
	mux.HandleFunc("GET /api/v1/gpus/{id}/telemetry", h.getTelemetry)
	mux.HandleFunc("GET /healthz", h.ok)
	mux.HandleFunc("GET /readyz", h.ok)
	mux.HandleFunc("GET /metrics", h.metrics.handler)
	return h.metrics.middleware(mux)
}

func (h *Handler) listGPUs(w http.ResponseWriter, r *http.Request) {
	gpus, err := h.store.ListGPUs(r.Context())
	if err != nil {
		h.logger.WithError(err).Error("failed to list GPUs")
		writeError(w, http.StatusInternalServerError, "failed to list GPUs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"gpus": gpus, "count": len(gpus)})
}

func (h *Handler) getTelemetry(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing GPU id")
		return
	}

	q := store.Query{Metric: r.URL.Query().Get("metric")}
	if v := r.URL.Query().Get("start_time"); v != "" {
		ts, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid start_time; expected RFC3339")
			return
		}
		q.Start = ts
	}
	if v := r.URL.Query().Get("end_time"); v != "" {
		ts, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid end_time; expected RFC3339")
			return
		}
		q.End = ts
	}
	if !q.Start.IsZero() && !q.End.IsZero() && q.End.Before(q.Start) {
		writeError(w, http.StatusBadRequest, "end_time is before start_time")
		return
	}

	samples, err := h.store.Telemetry(r.Context(), id, q)
	if err != nil {
		h.logger.WithError(err).WithField("gpu", id).Error("failed to query telemetry")
		writeError(w, http.StatusInternalServerError, "failed to query telemetry")
		return
	}

	// Distinguish an unknown GPU from a GPU with no samples in the window.
	if len(samples) == 0 && q.Start.IsZero() && q.End.IsZero() && q.Metric == "" {
		if !h.gpuExists(r, id) {
			writeError(w, http.StatusNotFound, "GPU not found")
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"gpu": id, "samples": samples, "count": len(samples)})
}

func (h *Handler) gpuExists(r *http.Request, id string) bool {
	gpus, err := h.store.ListGPUs(r.Context())
	if err != nil {
		return true // fail open: avoid a false 404 on a transient store error
	}
	for _, g := range gpus {
		if g.UUID == id {
			return true
		}
	}
	return false
}

func (h *Handler) ok(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

