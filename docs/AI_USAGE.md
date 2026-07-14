# AI Usage

This project was built with heavy use of an AI coding assistant (GitHub Copilot,
Claude). This document is an honest account of **how** AI was used, **what it produced
well**, and **where it fell short and needed human direction** — so a reviewer can see
the real division of labor rather than a marketing summary.

## Summary

AI was used as a pair-programming partner across the whole lifecycle: designing the
architecture, writing every component and its tests, generating the Kubernetes/Helm
assets, and producing the documentation. The human drove the requirements, made the
key architectural decisions, questioned the AI's output, caught its mistakes, and
performed all live validation on a real `kind` cluster.

A useful way to frame it: **AI produced most of the code and prose; the human owned
the decisions, the correctness checks, and the judgment.**

## Where AI was used

- **Design.** Drafting `docs/DESIGN.md` — the architecture, the custom message-queue
  internals (partitioning, consumer groups, delivery semantics, backpressure), the
  database choice comparison, and the tradeoffs table. AI produced strong first drafts;
  the human reshaped structure, trimmed wordiness, and corrected claims to match the
  implementation.
- **Implementation.** All five components — streamer, message queue (custom TCP
  broker), collector, API gateway, and the database assets — were written with AI,
  including the length-prefixed wire protocol, partition/consumer-group logic,
  at-least-once delivery with committed offsets, backpressure, the pgx persistence
  layer, and the REST + auto-generated OpenAPI surface.
- **Tests.** Table-driven unit tests co-located with each package, reaching ~90%+
  coverage on the logic packages (e.g. group 100%, partition 95%, wire 93%, parser 94%,
  pipeline 91%, api ~94%). AI wrote the bulk; the human set the coverage targets and the
  "don't test `main`, cover `internal/*`" convention.
- **Ops assets.** Dockerfiles (multi-stage, distroless), five Helm charts, the kind
  config, the Makefile targets (`build`/`test`/`cover`/`deploy`/`smoke`/`teardown`/
  `stream-start`/`stream-stop`), and the OpenAPI generation.
- **Documentation.** README, DESIGN, this file, and the Swagger UI integration.
- **Explanation.** A large share of the collaboration was the AI explaining its own
  design — sharding math, StatefulSet vs Deployment, offset semantics — which doubled
  as a review mechanism (explaining forces the reasoning to be checked).

## Where AI fell short (and the human had to intervene)

Being candid about the limits:

- **Architectural decisions were human-owned.** The AI would implement whatever was
  asked; it did not independently insist on the choices that mattered. The human decided:
  a single Go module (not multi-module), one root Makefile (not per-component), the
  **long-format** data model (store generic samples, key on `uuid`), and keeping the
  custom queue a genuine from-scratch TCP broker rather than leaning on a framework.
- **Correctness bugs.** AI introduced a SQL type typo (`BIGGABLE` → `BIGINT`) and left
  an unused test helper that failed the build; both were caught and fixed by the human.
- **A flaky test.** The consumer-group load-sharing test was timing-dependent and failed
  intermittently; the fix (subscribe both consumers and let the rebalance settle before
  publishing) came from human diagnosis, then stress-testing to confirm.
- **Environment/version friction.** Aligning the Go toolchain with the `golang:1.25`
  Docker base and `go.mod`, and working around Go not being on `PATH`, needed human
  steering.
- **A real design boundary surfaced only under questioning.** During validation the
  human asked "if I scale the streamer, does the new pod do work?" — which exposed that
  `kubectl scale` alone leaves a new streamer idle (its `REPLICAS` env is static), so
  streamers must be scaled via Helm. The AI had not flagged this proactively; it was
  found by the human probing behavior, then documented as a known boundary.
- **Over-verbosity.** AI drafts trended wordy and occasionally over-engineered; repeated
  human direction ("keep it simple", "is this too wordy?") was needed to land a clean,
  minimal result.

## Notable prompts that shaped the work

A selection of the human prompts that most influenced the result (not an exhaustive
list) — the ones where a requirement or a sharp question materially improved the system:

- **"Build an elastic, scalable, stable telemetry pipeline for an AI cluster with a
  *custom* message queue — no Kafka/RabbitMQ/ZeroMQ."** The core brief. Framing the
  queue as from-scratch is what made the wire protocol, partitions, consumer groups, and
  at-least-once delivery the heart of the project rather than a library call.

- **"Cover ~90% of test cases, keep the code simple and readable — small functions,
  optimal approach, logs at relevant places and comments — and use logrus so logging is
  configurable via the ConfigMap."** This single directive set conventions applied
  uniformly across all five components: logrus + an `internal/config` package
  (`LOG_LEVEL` / `LOG_FORMAT`), ~90% coverage on the logic packages, and a deliberately
  small, readable style.

- **"There should be a way to trigger and stop the telemetry flow, controlled by us."**
  Led to the `make stream-start` / `stream-stop` controls (scaling the producer), which
  made demos and testing far cleaner.

- **"Provide support to access the API using Swagger and get the data."** Led to serving
  Swagger UI at `/docs` and the live spec at `/openapi.yaml`, so the API is explorable
  from a browser with zero tooling.

- **"If a reviewer blindly follows the README, will they be able to run this?"**
  Triggered a runnability audit that fixed the prerequisites (make + bash + Docker daemon)
  and the deploy re-run note — turning "should work" into "does work".

- **"When I scale the streamer, why doesn't the new pod do anything?"** Exposed a genuine
  design boundary: `kubectl scale` leaves a new streamer idle because `REPLICAS` is
  static. This became a documented known boundary (scale streamers via Helm) plus a
  future-improvement (runtime replica discovery) — a case where a sharp question improved
  the design's honesty.

- **"Is the rebalance actually happening — how do I verify it?"** Pushed for observable
  proof, surfacing the broker's debug-level "partition assignment changed" log as the way
  to *see* partitions redistribute (8/8 -> 6/5/5 -> 16) rather than just trust it.

- **"Is this too wordy? Keep it simple."** Repeated throughout, this steady pressure kept
  both the code and the documentation concise instead of bloated.

## Verification (human-owned)

All correctness claims were checked by the human, not taken on faith:

- Full `go build ./...`, `go vet ./...`, and `go test ./...` run locally.
- **Live end-to-end validation on a real `kind` cluster** (an Ubuntu server with all
  dependencies installed): deploying all five components, confirming ingestion into
  PostgreSQL, exercising the API and Swagger UI, and testing the hard cases —
  consumer-group rebalancing across scale events, collector failover / at-least-once
  redelivery, producer flow control, and broker-restart recovery. See
  [VALIDATION.md](VALIDATION.md) for the full runbook and observed results.

## Honest assessment

AI dramatically accelerated the work — scaffolding, boilerplate, tests, and prose that
would otherwise take much longer. But it was an accelerator, not an autopilot: it needed
a human to set the requirements, make the design calls, catch its bugs, question its
assumptions, insist on simplicity, and prove the system actually works on a real
cluster. The final result reflects that partnership.
