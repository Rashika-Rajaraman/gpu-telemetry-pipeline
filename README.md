# Elastic GPU Telemetry Pipeline

An elastic, scalable telemetry pipeline for an AI/GPU cluster, built around a
**custom message queue** (no ZeroMQ/RabbitMQ/Kafka). Telemetry is streamed from
DCGM CSV data, transported over the custom queue, persisted, and exposed via a
REST API. Everything deploys to Kubernetes (kind) via Helm.

> Status: **implemented** — all 5 components are built, unit-tested, and the module
> builds cleanly (`go build ./...`, `go vet ./...`). The OpenAPI spec is
> auto-generated from code. Container/kind deployment assets are provided.

## Components (5 independently built, imaged, and deployed)

| Component | Directory | Role |
|---|---|---|
| Streamer | `streamer/` | Reads DCGM CSV, shards rows, publishes to the queue |
| Message Queue | `messagequeue/` | Custom TCP broker: partitions, consumer groups, acks, backpressure |
| Collector | `collector/` | Consumes, parses DCGM lines, persists samples |
| API Gateway | `apigateway/` | REST API + auto-generated OpenAPI |
| Database | `database/` | PostgreSQL schema (the collector↔API contract) |

## Repository layout

```
telemetry-pipeline/
├── go.mod                     # single module (build-time); 5 independent binaries at runtime
├── Makefile                   # build · test · cover · openapi · docker · kind · helm
├── data/                      # sample DCGM CSV
├── scripts/                   # kind-up, image load, openapi gen
├── deployment/                # kind config + 5 standalone Helm charts
├── streamer/  messagequeue/  collector/  apigateway/  database/
└── api/openapi.yaml           # generated
```

Each Go component is self-contained: its own `cmd/`, `internal/`, and tests.
The only shared code is the queue's public client SDK (`messagequeue/client`).

## Architecture

```
DCGM CSV → [Streamer × N] → (partition by GPU uuid) → [Message Queue]
             → (consumer group) → [Collector × M] → PostgreSQL ← [API Gateway] ← clients
```

- Partitioning by GPU `uuid` preserves per-GPU ordering end to end.
- Consumer groups let collectors scale and rebalance elastically.
- At-least-once delivery with acks + redelivery; bounded buffers apply backpressure.

See **Design considerations** below for the full rationale.

## Data model

DCGM data is **long-format** (one metric per row). Each row becomes a generic
sample: `{timestamp, metric, value, uuid, gpu_index, device, model, hostname}`.
The GPU identity is the globally-unique `uuid`; the per-host `gpu_id` index is not
unique across hosts. Per spec, the **timestamp is assigned at stream time**.

## API

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/gpus` | List all GPUs with telemetry |
| GET | `/api/v1/gpus/{id}/telemetry` | Samples for a GPU (`id` = uuid), ordered by time |
| GET | `/api/v1/gpus/{id}/telemetry?start_time=&end_time=&metric=` | Filtered |

OpenAPI spec: `api/openapi.yaml` (regenerate with `make openapi`).

## Build & test

```bash
make build      # build all binaries
make test       # unit tests
make cover      # coverage summary + coverage.html
make openapi    # regenerate OpenAPI spec
make lint       # go vet
```

## Deploy to kind

```bash
make deploy         # one-shot: cluster + images + charts + rollout wait
make smoke          # verify the API end to end
make teardown       # delete the cluster

# or run the phases individually:
make kind-up        # create the local cluster
make images-load    # build the 4 component images and load them into kind
make helm-install   # install all charts (database first, then components)
```

## AI assistance

This project was developed with heavy AI assistance (GitHub Copilot). The full
prompt-by-prompt account — what was AI-generated, what needed manual fixes, and
where prompts fell short — is in [docs/AI_USAGE.md](docs/AI_USAGE.md). A short
summary appears at the end of this README.

## Design considerations

> Full design document: **[docs/DESIGN.md](docs/DESIGN.md)** — problem framing,
> component designs, MQ internals, delivery semantics, failure modes, alternatives
> considered, and future work. The section below is a condensed summary.

### The custom message queue (the core)

The broker is a from-scratch TCP server (`messagequeue/`), no third-party MQ. Its
wire protocol (`internal/wire`) is length-prefixed binary frames: a 4-byte length,
a 1-byte opcode, and a JSON payload. gRPC/protobuf were deliberately avoided to
keep the queue genuinely “custom” and dependency-free.

- **Partitions & ordering.** Each topic has a fixed set of partitions. A produced
  message is routed by `hash(key) % N`, and the streamer uses the GPU `uuid` as
  the key. So all telemetry for a GPU lands on one partition and is delivered in
  produce order — giving **per-GPU ordering end to end**, which is exactly what
  the `telemetry ordered by time` API needs.
- **Consumer groups & elasticity.** Collectors join a group; the broker assigns
  partitions across members and **rebalances** on every join/leave. Scaling
  collectors up or down redistributes partitions automatically (competing
  consumers — each message processed once per group).
- **At-least-once delivery.** The broker delivers from a group's committed offset
  and only advances it when the consumer acks. If a collector dies mid-batch, the
  committed offset hasn't moved, so the partition's new owner redelivers. No
  separate durable cursor is needed. Idempotent DB upserts (`ON CONFLICT`) make
  redelivery safe.
- **Backpressure & memory safety.** Each partition is a bounded in-memory log.
  Records are retained only until the *minimum* committed offset across groups;
  when the buffer is full, `Append` blocks, which withholds the producer's ack and
  throttles it. Memory can't grow unbounded — a slow/absent consumer slows
  producers rather than exhausting the broker.

### Scale, performance, availability

- Scale target is ≤10 streamers/collectors. A fixed partition count (16 by
  default) exceeds that, so load spreads evenly.
- Delivery is pipelined per partition with batched frames; JSON keeps the protocol
  debuggable at telemetry rates.
- **Availability caveat (documented trade-off):** the broker is a single instance
  (SPOF) and state is in-memory. For the exercise scope this is acceptable. A
  production path would add a write-ahead log for durability and follower brokers
  with partition replication + leader election for HA.

### Data model

DCGM export is **long-format** (one metric per row), so each row is stored as a
generic sample rather than pivoted into wide records — faithful to the source and
trivially extensible to new metrics. GPU identity is the globally-unique `uuid`
(the per-host `gpu_id` index repeats across hosts). The `gpu_samples` table is
indexed on `(uuid, ts)` for the primary query, with a unique `(uuid, metric, ts)`
index providing idempotency.

### Component isolation

The five components are compiler-isolated: each owns its `cmd/` and `internal/`,
and Go forbids importing another component's `internal/`. The only shared code is
the queue's public client SDK (`messagequeue/client`). Other boundaries are
contracts, not code: the streamer→collector contract is the raw CSV line + uuid
key; the collector→API contract is the database schema; the client→API contract is
OpenAPI.

### Observability & error handling

- Structured logging via **logrus** in every binary; log level and format are
  configurable through the Kubernetes ConfigMap (`LOG_LEVEL`, `LOG_FORMAT`)
  without a rebuild.
- API gateway exposes `/healthz`, `/readyz`, and a dependency-free `/metrics`
  (Prometheus text) with request/error counters.
- Every service retries its dependencies with backoff (broker/DB may start later
  in Kubernetes) and shuts down gracefully on SIGTERM.
- Malformed CSV rows are skipped (not fatal); bad API input returns 400, unknown
  GPUs 404, backend failures 500 with a JSON error envelope.

## Testing & coverage

Every component ships table-driven unit tests co-located with its code, runnable
offline (in-memory store + fake MQ/consumer, no Postgres or broker needed):

```bash
make test     # run all unit tests
make cover    # aggregated coverage: summary + coverage.html
```

Highlights: MQ backpressure/rebalance/ordering, two-consumer load sharing,
sharding correctness, idempotent persistence, and full API handler coverage
(filters, 404/400, metrics). Postgres implementations are covered by
build-tagged integration tests (`//go:build integration`) that require a live DB.

## Installation workflow (Kubernetes / kind)

Requires Docker, kind, kubectl, and Helm.

```bash
make deploy         # create cluster, build+load images, install charts, wait
kubectl get pods    # all components Running
make smoke          # curl the API to confirm data is flowing

# equivalent explicit steps:
make kind-up        # create the local cluster
make images-load    # build the 4 component images and load them into kind
make helm-install   # install database, broker, collector, streamer, apigateway
```

PostgreSQL uses the stock `postgres:16-alpine` image (pulled by kind on demand)
with the schema applied on first start; the four Go components are loaded via
`kind load`, so no external registry is required.

## Sample user workflow

```bash
# API is exposed at localhost:8080 via the kind NodePort mapping.
curl localhost:8080/api/v1/gpus
curl "localhost:8080/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry"
curl "localhost:8080/api/v1/gpus/GPU-5fd4.../telemetry?start_time=2025-07-18T20:42:00Z&end_time=2025-07-18T20:43:00Z&metric=DCGM_FI_DEV_GPU_UTIL"

# Scale streamers/collectors elastically:
kubectl scale statefulset streamer --replicas=4
kubectl scale deployment collector --replicas=5
```

## Limitations & future work

- Single broker instance (SPOF, in-memory state) — see availability note above.
- No auth/TLS on the API or broker (out of scope).
- Postgres credentials are demo values in Helm; use a Secret in production.

## AI usage (summary)

AI (GitHub Copilot) drove most of the work: repo/design bootstrapping, all
component code, and the unit tests. Manual intervention was needed for a handful
of things — a SQL type typo, an accidental unused test helper, aligning the Go
toolchain/Dockerfile versions, and steering several design decisions (single vs.
multi module, one vs. per-component Makefile, the long-format data model). Full
detail, including verbatim prompts and where they fell short, is in
[docs/AI_USAGE.md](docs/AI_USAGE.md).
