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

## Verification (human-owned)

All correctness claims were checked by the human, not taken on faith:

- Full `go build ./...`, `go vet ./...`, and `go test ./...` run locally.
- **Live end-to-end validation on a real `kind` cluster** (Docker Desktop + WSL2):
  deploying all five components, confirming ingestion into PostgreSQL, exercising the
  API and Swagger UI, and testing the hard cases — consumer-group rebalancing across
  scale events, collector failover / at-least-once redelivery, producer flow control,
  and broker-restart recovery. See [VALIDATION.md](VALIDATION.md) for the full runbook
  and observed results.

## Honest assessment

AI dramatically accelerated the work — scaffolding, boilerplate, tests, and prose that
would otherwise take much longer. But it was an accelerator, not an autopilot: it needed
a human to set the requirements, make the design calls, catch its bugs, question its
assumptions, insist on simplicity, and prove the system actually works on a real
cluster. The final result reflects that partnership.
