package api

import (
	"fmt"
	"net/http"
	"sync"
)

// metrics is a minimal Prometheus-style counter set exposed at /metrics. It avoids
// an external metrics dependency while still giving basic observability into
// request volume and error rate.
type metrics struct {
	mu       sync.Mutex
	requests int64
	errors   int64
	byStatus map[int]int64
}

func newMetrics() *metrics {
	return &metrics{byStatus: make(map[int]int64)}
}

// statusRecorder captures the response status for the metrics middleware.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// middleware records request counts and status codes.
func (m *metrics) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		m.mu.Lock()
		m.requests++
		m.byStatus[rec.status]++
		if rec.status >= 500 {
			m.errors++
		}
		m.mu.Unlock()
	})
}

// handler serves the counters in Prometheus text exposition format.
func (m *metrics) handler(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP apigateway_http_requests_total Total HTTP requests.\n")
	fmt.Fprintf(w, "# TYPE apigateway_http_requests_total counter\n")
	fmt.Fprintf(w, "apigateway_http_requests_total %d\n", m.requests)
	fmt.Fprintf(w, "# HELP apigateway_http_errors_total Total HTTP 5xx responses.\n")
	fmt.Fprintf(w, "# TYPE apigateway_http_errors_total counter\n")
	fmt.Fprintf(w, "apigateway_http_errors_total %d\n", m.errors)
	fmt.Fprintf(w, "# HELP apigateway_http_responses_by_status Responses by status code.\n")
	fmt.Fprintf(w, "# TYPE apigateway_http_responses_by_status counter\n")
	for code, n := range m.byStatus {
		fmt.Fprintf(w, "apigateway_http_responses_by_status{code=\"%d\"} %d\n", code, n)
	}
}
