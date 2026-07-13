#!/usr/bin/env bash
# Build the four Go component images and load them directly into the kind cluster.
# `kind load docker-image` copies each image into every kind node, and pods
# reference them with imagePullPolicy: IfNotPresent, so no registry is needed.
# PostgreSQL uses the stock postgres:16-alpine image, which kind pulls on demand.
set -euo pipefail

REGISTRY="${REGISTRY:-localhost:5001}"
TAG="${TAG:-dev}"
CLUSTER_NAME="${CLUSTER_NAME:-telemetry}"
COMPONENTS=(streamer messagequeue collector apigateway)

for c in "${COMPONENTS[@]}"; do
  echo ">> building ${c}"
  docker build -f "${c}/Dockerfile" -t "${REGISTRY}/${c}:${TAG}" .
  echo ">> loading ${c} into kind"
  kind load docker-image "${REGISTRY}/${c}:${TAG}" --name "${CLUSTER_NAME}"
done

echo ">> all component images loaded into kind cluster '${CLUSTER_NAME}'"
