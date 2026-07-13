// Package openapi generates the OpenAPI 3.0 document for the telemetry API from
// Go definitions, so the spec is auto-generated (no hand-written YAML) and stays
// in sync with the handlers. `make openapi` renders it to api/openapi.yaml.
package openapi

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Version is the API version advertised in the spec.
const Version = "1.0.0"

// Document returns the OpenAPI 3.0 document as an ordered structure.
func Document() *yaml.Node {
	doc := map[string]any{
		"openapi": "3.0.3",
		"info": map[string]any{
			"title":       "GPU Telemetry API",
			"description": "Query GPU telemetry collected from an AI cluster.",
			"version":     Version,
		},
		"servers": []any{
			map[string]any{"url": "/", "description": "API gateway"},
		},
		"paths": map[string]any{
			"/api/v1/gpus": map[string]any{
				"get": map[string]any{
					"summary":     "List all GPUs",
					"description": "Returns every GPU for which telemetry is available.",
					"operationId": "listGPUs",
					"responses": map[string]any{
						"200": map[string]any{
							"description": "List of GPUs",
							"content": jsonContent(map[string]any{
								"type": "object",
								"properties": map[string]any{
									"count": map[string]any{"type": "integer"},
									"gpus": map[string]any{
										"type":  "array",
										"items": ref("GPU"),
									},
								},
							}),
						},
					},
				},
			},
			"/api/v1/gpus/{id}/telemetry": map[string]any{
				"get": map[string]any{
					"summary":     "Query telemetry by GPU",
					"description": "Returns telemetry samples for a GPU (by uuid), ordered by time, with optional time-window and metric filters.",
					"operationId": "getTelemetry",
					"parameters": []any{
						pathParam("id", "GPU uuid"),
						queryParam("start_time", "Inclusive lower time bound (RFC3339)", "date-time"),
						queryParam("end_time", "Inclusive upper time bound (RFC3339)", "date-time"),
						queryParam("metric", "Restrict to a single DCGM metric name", ""),
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Telemetry samples",
							"content": jsonContent(map[string]any{
								"type": "object",
								"properties": map[string]any{
									"gpu":   map[string]any{"type": "string"},
									"count": map[string]any{"type": "integer"},
									"samples": map[string]any{
										"type":  "array",
										"items": ref("Sample"),
									},
								},
							}),
						},
						"400": map[string]any{"description": "Invalid query parameter", "content": jsonContent(ref("Error"))},
						"404": map[string]any{"description": "GPU not found", "content": jsonContent(ref("Error"))},
					},
				},
			},
		},
		"components": map[string]any{
			"schemas": map[string]any{
				"GPU": object(map[string]any{
					"uuid":       str(),
					"gpu_index":  str(),
					"device":     str(),
					"model_name": str(),
					"hostname":   str(),
				}),
				"Sample": object(map[string]any{
					"timestamp": strFmt("date-time"),
					"metric":    str(),
					"value":     map[string]any{"type": "number"},
				}),
				"Error": object(map[string]any{
					"error": str(),
				}),
			},
		},
	}

	var node yaml.Node
	// Marshal→unmarshal into a yaml.Node so callers can render deterministically.
	raw, _ := yaml.Marshal(doc)
	_ = yaml.Unmarshal(raw, &node)
	return &node
}

// YAML renders the OpenAPI document to YAML bytes.
func YAML() ([]byte, error) {
	out, err := yaml.Marshal(Document())
	if err != nil {
		return nil, fmt.Errorf("openapi: marshal: %w", err)
	}
	return out, nil
}

// --- small schema helpers -------------------------------------------------

func jsonContent(schema any) map[string]any {
	return map[string]any{"application/json": map[string]any{"schema": schema}}
}

func ref(name string) map[string]any {
	return map[string]any{"$ref": "#/components/schemas/" + name}
}

func object(props map[string]any) map[string]any {
	return map[string]any{"type": "object", "properties": props}
}

func str() map[string]any { return map[string]any{"type": "string"} }

func strFmt(format string) map[string]any {
	return map[string]any{"type": "string", "format": format}
}

func pathParam(name, desc string) map[string]any {
	return map[string]any{
		"name":        name,
		"in":          "path",
		"required":    true,
		"description": desc,
		"schema":      str(),
	}
}

func queryParam(name, desc, format string) map[string]any {
	schema := str()
	if format != "" {
		schema = strFmt(format)
	}
	return map[string]any{
		"name":        name,
		"in":          "query",
		"required":    false,
		"description": desc,
		"schema":      schema,
	}
}

