package api

import (
	"net/http"

	"github.com/gpu-telemetry-pipeline/api-gateway/internal/openapi"
)

// swaggerUIPage is a minimal Swagger UI shell. It loads the UI assets from a CDN and
// renders the spec served at /openapi.yaml, giving reviewers an interactive API
// explorer where "Try it out" calls the gateway on the same origin.
const swaggerUIPage = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>GPU Telemetry API - Swagger UI</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
  <script>
    window.onload = function () {
      window.ui = SwaggerUIBundle({ url: '/openapi.yaml', dom_id: '#swagger-ui' });
    };
  </script>
</body>
</html>`

// openAPISpec serves the auto-generated OpenAPI document as YAML.
func (h *Handler) openAPISpec(w http.ResponseWriter, _ *http.Request) {
	spec, err := openapi.YAML()
	if err != nil {
		h.logger.WithError(err).Error("failed to render OpenAPI spec")
		writeError(w, http.StatusInternalServerError, "failed to render OpenAPI spec")
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(spec)
}

// swaggerUI serves the interactive Swagger UI explorer.
func (h *Handler) swaggerUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(swaggerUIPage))
}
