# System Validation

This document is a repeatable runbook for validating the telemetry pipeline end to end
on a local `kind` cluster, plus a record of the results observed during a live run. It
proves the three qualities in the brief — **elastic, scalable, stable** — around the
custom message queue.

## Prerequisites

- Docker (daemon running), `kind`, `kubectl`, `helm`, `make`, and a bash shell
  (Linux, macOS, or WSL2 + Docker Desktop).
- Optional: `jq` for readable JSON (all `jq` uses below are optional).

Bring the system up first:

```bash
make deploy      # cluster + images + charts + rollout wait
```

> Tip: enable verbose collector/broker logs during testing with
> `kubectl set env deployment/collector LOG_LEVEL=debug` and
> `kubectl set env deployment/message-queue LOG_LEVEL=debug`, and reset them to `info`
> when finished.

---

## Phase 1 — Baseline health

```bash
kubectl get pods
curl -s localhost:8080/healthz     # {"status":"ok"}
curl -s localhost:8080/readyz      # {"status":"ok"}
```

**Pass:** all pods `Running` (database, message-queue, collector×2, api-gateway,
streamer×2) with low restarts; both probes return `ok`.

## Phase 2 — End-to-end data flow

```bash
kubectl exec statefulset/database -- psql -U telemetry -d telemetry \
  -c "SELECT count(*), count(distinct uuid) FROM gpu_samples;"
sleep 10
kubectl exec statefulset/database -- psql -U telemetry -d telemetry \
  -c "SELECT count(*), count(distinct uuid) FROM gpu_samples;"
```

**Pass:** the row count grows between the two queries (continuous ingestion), and the
distinct-uuid count matches the number of GPUs in the sample CSV and stays constant.

## Phase 3 — API functional tests

```bash
GPU=$(curl -s localhost:8080/api/v1/gpus | jq -r '.gpus[0].uuid')
curl -s localhost:8080/api/v1/gpus | jq '.count'
curl -s "localhost:8080/api/v1/gpus/$GPU/telemetry" | jq '.count'
curl -s "localhost:8080/api/v1/gpus/$GPU/telemetry?metric=DCGM_FI_DEV_GPU_UTIL" | jq '.samples[0]'
curl -s "localhost:8080/api/v1/gpus/$GPU/telemetry?start_time=2020-01-01T00:00:00Z&end_time=2020-01-02T00:00:00Z" | jq '.count'
curl -s -o /dev/null -w "%{http_code}\n" "localhost:8080/api/v1/gpus/GPU-nope/telemetry"     # 404
curl -s -o /dev/null -w "%{http_code}\n" "localhost:8080/api/v1/gpus/$GPU/telemetry?start_time=bad"  # 400
```

**Pass:** GPU list non-empty; telemetry count > 0; metric filter returns only that
metric; an out-of-range time window returns count `0` (known GPU, no samples — not a
404); unknown GPU → `404`; malformed time → `400`.

## Phase 4 — Elastic scaling (collectors, via `kubectl scale`)

```bash
kubectl set env deployment/message-queue LOG_LEVEL=debug
kubectl scale deployment collector --replicas=1   # then 2, then 3
kubectl logs -f -l app=message-queue | grep "partition assignment"
```

**Pass:** the broker rebalances so all 16 partitions stay covered and are split evenly
across the current collectors — 16 on 1 consumer, 8/8 on 2, 6/5/5 on 3 — and ingestion
never stops. Collectors scale via `kubectl scale` because the broker assigns their work
at runtime.

## Phase 5 — Failover & at-least-once

```bash
kubectl exec statefulset/database -- psql -U telemetry -d telemetry -c "SELECT count(*) FROM gpu_samples;"
POD=$(kubectl get pods -l app=collector -o jsonpath='{.items[0].metadata.name}')
kubectl delete pod "$POD"        # optionally: --force --grace-period=0 to simulate a crash
kubectl logs -l app=message-queue --tail=15
kubectl exec statefulset/database -- psql -U telemetry -d telemetry -c "SELECT count(*), count(distinct uuid) FROM gpu_samples;"
```

**Pass:** the broker reassigns the dead consumer's partitions to survivors; a
replacement pod rejoins (Deployment self-heals); the row count keeps growing (no loss);
no duplicate rows appear because redelivered records are absorbed by the unique
`(uuid, metric, ts)` constraint. This exercises the *persist-then-ack* ordering: a crash
before ack causes safe reprocessing, never loss.

## Phase 6 — Streamer scaling (via Helm, not `kubectl scale`)

```bash
helm upgrade --install streamer deployment/helm/streamer \
  --set image.repository=localhost:5001/streamer --set image.tag=dev \
  --set replicaCount=3
kubectl rollout status statefulset/streamer --timeout=120s
kubectl logs streamer-2 --tail=5
```

**Pass:** three streamers `Running` with `REPLICAS=3`; the new pod publishes its shard
(`idx % 3 == 2`); aggregate ingest rate rises.

**Known boundary (optional demo):** `kubectl scale statefulset streamer --replicas=4`
adds `streamer-3`, but it publishes **nothing** because its `REPLICAS` env is still `3`
(`idx % 3 == 3` never matches). Streamers must be scaled through Helm so `REPLICAS` is
re-rendered. See DESIGN.md §4.1.

## Phase 7 — Flow control (pause / resume)

```bash
make stream-stop        # scale streamer to 0
# row count holds steady:
kubectl exec statefulset/database -- psql -U telemetry -d telemetry -c "SELECT count(*) FROM gpu_samples;"
sleep 20
kubectl exec statefulset/database -- psql -U telemetry -d telemetry -c "SELECT count(*) FROM gpu_samples;"
make stream-start       # resume (STREAM_REPLICAS, default 2)
```

**Pass:** while stopped, no streamer pods and the row count is flat; the rest of the
pipeline stays `Running` (idle); after start, ingestion resumes.

## Phase 8 — Broker restart (in-memory tradeoff & recovery)

```bash
kubectl rollout restart deployment/message-queue
kubectl rollout status deployment/message-queue --timeout=120s
kubectl logs -l app=streamer  --tail=10    # "broker not ready, retrying" → "connected"
kubectl logs -l app=collector --tail=10    # reconnect & resume
```

**Pass:** a fresh broker starts with empty in-memory state; streamers and collectors
reconnect via their retry/backoff logic; ingestion resumes. Any records buffered in the
broker but not yet consumed are lost by design (documented in DESIGN.md §5.5) — the
system self-heals without manual intervention.

## Phase 9 — Swagger UI & metrics

```bash
curl -s localhost:8080/openapi.yaml | head          # served OpenAPI 3.0 spec
curl -s localhost:8080/metrics | grep apigateway_http_requests_total
```

Then open **http://localhost:8080/docs** → expand `GET /api/v1/gpus` → **Try it out** →
**Execute**.

**Pass:** the spec is served; `/metrics` shows request counters; Swagger UI renders the
two API operations and "Try it out" returns live data. (Swagger UI loads assets from a
CDN, so the browser needs internet access.)

## Phase 10 — Cleanup

```bash
kubectl set env deployment/collector LOG_LEVEL=info
kubectl set env deployment/message-queue LOG_LEVEL=info
make teardown        # delete the kind cluster
```

---

## Observed results (live run)

A full live run was executed on an Ubuntu server with all dependencies (Docker, kind,
kubectl, Helm) installed. Highlights:

- **Ingestion:** row count climbed continuously (e.g. 12,849 → 15,725 over a few
  minutes), distinct GPU count stable — confirming end-to-end flow into PostgreSQL.
- **API:** `/api/v1/gpus` returned all GPUs; telemetry queries returned ordered samples;
  metric/time filters worked; unknown GPU → 404, bad time → 400; out-of-range window → 0.
- **Rebalance:** scaling collectors produced clean, even partition assignments —
  `8/8` on two consumers, `6/5/5` on three, `16` on one — with every partition 0–15
  always owned and ingestion uninterrupted.
- **Failover:** deleting a collector mid-stream rebalanced its partitions onto survivors,
  a replacement pod rejoined, and the row count kept growing with no duplicates.
- **Flow control:** `stream-stop` held the row count flat; `stream-start` resumed it.
- **Broker restart:** components reconnected and ingestion resumed automatically.
- **Swagger UI:** `/docs` rendered the two operations and three schemas (GPU, Sample,
  Error); "Try it out" returned live data.

All target properties — elastic (scale up/down), scalable (partitioned parallelism),
and stable (failover, backpressure, graceful recovery) — were demonstrated live.
