package openapi

import (
	"strings"
	"testing"
)

func TestYAMLContainsPathsAndParams(t *testing.T) {
	out, err := YAML()
	if err != nil {
		t.Fatalf("YAML: %v", err)
	}
	s := string(out)

	for _, want := range []string{
		"openapi: 3.0.3",
		"/api/v1/gpus",
		"/api/v1/gpus/{id}/telemetry",
		"start_time",
		"end_time",
		"metric",
		"operationId: listGPUs",
		"operationId: getTelemetry",
		"GPU",
		"Sample",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("generated spec missing %q\n---\n%s", want, s)
		}
	}
}

func TestYAMLIsNonEmpty(t *testing.T) {
	out, err := YAML()
	if err != nil {
		t.Fatal(err)
	}
	if len(out) < 200 {
		t.Fatalf("spec suspiciously short: %d bytes", len(out))
	}
}
