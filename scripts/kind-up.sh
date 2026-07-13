#!/usr/bin/env bash
# Create the local kind cluster for the telemetry pipeline. Component images are
# loaded directly with `kind load` (see load-images.sh), so no separate container
# registry is required.
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-telemetry}"

echo ">> creating kind cluster '${CLUSTER_NAME}'"
kind create cluster --name "${CLUSTER_NAME}" --config deployment/kind/kind-config.yaml

echo ">> cluster ready"
kubectl cluster-info --context "kind-${CLUSTER_NAME}"
