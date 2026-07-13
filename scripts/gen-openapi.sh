#!/usr/bin/env bash
# Regenerate the OpenAPI spec from the API gateway definitions.
# Invoked by `make openapi`.
set -euo pipefail

go run ./api-gateway/cmd --dump-openapi > api/openapi.yaml
echo ">> wrote api/openapi.yaml"
