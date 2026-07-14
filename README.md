# Elastic GPU Telemetry Pipeline

An elastic, scalable telemetry pipeline for an AI/GPU cluster, built around a
**custom message queue** (no ZeroMQ/RabbitMQ/Kafka). Telemetry is streamed from
DCGM CSV data, transported over the custom queue, persisted to PostgreSQL, and
exposed via a REST API. Everything deploys to Kubernetes (kind) via Helm.

## Components

| Component | Directory | Role |
|---|---|---|
| Streamer | `streamer/` | Reads DCGM CSV, shards rows, publishes to the queue |
| Message Queue | `message-queue/` | Custom TCP broker: partitions, consumer groups, acks, backpressure |
| Collector | `collector/` | Consumes, parses DCGM lines, persists samples |
| API Gateway | `api-gateway/` | REST API + auto-generated OpenAPI |
| Database | `database/` | PostgreSQL schema (the collector↔API contract) |

Each Go component is self-contained (its own `cmd/`, `internal/`, and tests). The
only shared code is the queue's public client SDK (`message-queue/client`).

## Architecture

```
DCGM CSV → [Streamer × N] → (partition by GPU uuid) → [Message Queue]
             → (consumer group) → [Collector × M] → PostgreSQL ← [API Gateway] ← clients
```

Partitioning by GPU `uuid` preserves per-GPU ordering end to end, consumer groups
let collectors scale and rebalance elastically, and at-least-once delivery with
bounded-buffer backpressure keeps the pipeline stable under load.

**The full design — problem framing, component designs, message-queue internals,
delivery semantics, failure modes, alternatives considered, and future work — is
in [docs/DESIGN.md](docs/DESIGN.md).**

## Prerequisites

- A Unix-like shell with **GNU Make** (Linux, macOS, or WSL) — every command below is
  a `make` target, and the helper scripts are bash
- Go 1.25+ — for `make build` / `make test` / `make cover`
- A running **Docker** daemon, plus [kind](https://kind.sigs.k8s.io/), kubectl, and
  Helm — for deployment

## Build & test

```bash
make build      # build all component binaries into ./bin
make test       # run all unit tests
make cover      # aggregated coverage: summary + coverage.html
make lint       # go vet
make openapi    # regenerate api/openapi.yaml from the API gateway code
```

Unit tests run fully offline (in-memory store + fake queue/consumer — no Postgres
or broker required). Postgres-backed code is covered by build-tagged integration
tests (`//go:build integration`) that need a live database.

## Run the pipeline end to end (kind)

Requires Docker, kind, kubectl, and Helm.

```bash
make deploy     # create cluster, build+load images, install all charts, wait for rollout
make smoke      # curl the API to confirm telemetry is flowing
make teardown   # delete the cluster
```

> Re-running `make deploy` on an existing cluster fails at cluster creation — run
> `make teardown` first, or use the individual phases below.

Run the phases individually if you prefer:

```bash
make kind-up        # create the local kind cluster
make images-load    # build the 4 component images and load them into kind
make helm-install   # install charts (database first, then components)
kubectl get pods    # wait until everything is Running
```

PostgreSQL uses the stock `postgres:16-alpine` image (pulled by kind on demand)
with the schema applied on first start; the four Go components are loaded via
`kind load`, so no external registry is required.

## Control the telemetry flow

The streamer is the only producer, so you can start and stop telemetry on demand:

```bash
make stream-stop     # pause: scale the streamer to 0 (row count stops growing)
make stream-start    # resume: scale the streamer back up (STREAM_REPLICAS, default 2)
```

While paused, the queue drains and collectors go idle; resuming continues from where
it left off.

## Use the API

The API is exposed at `localhost:8080` via the kind NodePort mapping. Explore and call
it interactively in **Swagger UI at [http://localhost:8080/docs](http://localhost:8080/docs)** —
the spec is served live at `/openapi.yaml` and also checked in at
[api/openapi.yaml](api/openapi.yaml).

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/gpus` | List all GPUs with telemetry |
| GET | `/api/v1/gpus/{id}/telemetry` | Samples for a GPU (`id` = uuid), ordered by time |
| GET | `/api/v1/gpus/{id}/telemetry?start_time=&end_time=&metric=` | Filtered by time window / metric |

```bash
curl localhost:8080/api/v1/gpus
curl "localhost:8080/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry"
curl "localhost:8080/api/v1/gpus/GPU-5fd4.../telemetry?start_time=2025-07-18T20:42:00Z&end_time=2025-07-18T20:43:00Z&metric=DCGM_FI_DEV_GPU_UTIL"
```

Scale the tiers elastically. **Collectors** rebalance instantly under `kubectl scale`
(the broker reassigns partitions at runtime):

```bash
kubectl scale deployment collector --replicas=5
```

**Streamers** must be scaled through Helm, not `kubectl scale`. A streamer's share of
the CSV depends on the `REPLICAS` value baked into its pods; `kubectl scale` adds a pod
without updating that value, so the new pod owns no rows and publishes nothing. Use
Helm so the count is re-rendered and the pods re-shard:

```bash
helm upgrade --install streamer deployment/helm/streamer \
  --set image.repository=localhost:5001/streamer --set image.tag=dev \
  --set replicaCount=4
```

See the "known boundary" note in [docs/DESIGN.md](docs/DESIGN.md) (Section 4.1) for why.

## Validation

A repeatable end-to-end validation runbook — health, data flow, API, consumer-group
rebalancing, failover/at-least-once, flow control, and broker-restart recovery — plus
the results observed on a live `kind` cluster, is in [docs/VALIDATION.md](docs/VALIDATION.md).

## AI usage

This project was built with heavy AI assistance (GitHub Copilot). An honest account —
what AI produced, what needed manual fixes, and where it fell short — is in
[docs/AI_USAGE.md](docs/AI_USAGE.md).
