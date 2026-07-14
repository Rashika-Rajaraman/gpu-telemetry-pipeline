# AI Usage

This project was built with extensive use of AI coding assistant (GitHub Copilot, Claude). This document provides an honest account of how AI was used, where it accelerated development, where it fell short, and where human judgment and intervention were required.

The goal is not to claim more or less AI usage than actually occurred, but to clearly communicate the division of labor between the AI tools and the engineer responsible for the system.

## Approach

The project was handled **design-first**: no code was written until the architecture was settled. Several alternatives for the message queue, delivery guarantees, partitioning, and storage were explored and challenged through discussion, then captured in a design document (`DESIGN.md`) that converged on a single approach. Only once the design was agreed did implementation begin.

Implementation then proceeded **component by component** against a shared set of conventions (configuration, logging, small functions, tests), rather than building everything at once — each component was implemented, unit-tested, and refined before moving to the next. Finally, the whole system was assembled and **validated end-to-end on a live Kubernetes cluster**, where scaling, failover, and recovery behavior were exercised and documented.

This order — design, then focused implementation, then live validation — kept the build deliberate and avoided rework.

## Summary

AI was used as a pair-programming partner throughout the entire lifecycle of the project:

- Architecture and design discussions
- Component implementation
- Unit test generation
- Kubernetes and Helm assets
- Documentation and README creation
- Validation planning and review

The human owned:

- Requirements and scope
- Architectural decisions
- Tradeoff analysis
- Correctness verification
- Failure testing
- Final validation on a live Kubernetes cluster

A useful way to frame the collaboration is:

> AI produced a substantial proportion of the initial code, tests, and documentation drafts; the human owned the requirements, architectural decisions, correctness checks, and final validation.

## Where AI Was Used

- **Design.** Drafting `docs/DESIGN.md` — architecture, the custom message-queue internals
  (partitioning, consumer groups, delivery semantics, backpressure), the database
  comparison, and the tradeoffs table. AI produced strong first drafts; the human reshaped
  structure and corrected claims to match the implementation.
- **Implementation.** All five components — streamer, message queue (custom TCP broker),
  collector, API gateway, and database assets — including the length-prefixed wire
  protocol, partition and consumer-group logic, at-least-once delivery with committed
  offsets, backpressure, the pgx persistence layer, and the REST + auto-generated OpenAPI
  surface.
- **Tests.** Table-driven unit tests co-located with each package, ~90%+ coverage on the
  logic packages. The human set the coverage targets, package boundaries, integration-test
  strategy, and the "don't test `main`, cover `internal/*`" convention.
- **Operations.** Multi-stage distroless Dockerfiles, five Helm charts, the kind config,
  the Makefile targets, OpenAPI generation, and the Swagger UI.
- **Documentation.** README, DESIGN, this file, and the validation runbook.
- **Explanation.** A large share of the work was AI explaining its own design — sharding
  math, StatefulSet vs Deployment, offset semantics — which doubled as a review mechanism,
  since explaining a design often exposes weak assumptions.

## Where AI Fell Short

- **Architectural decisions were human-owned.** AI implemented what it was asked but did
  not independently drive the choices that mattered: a single Go module (not
  multi-module), one generic root Makefile (not per-component), logrus over slog for
  ConfigMap-tunable logging, the long-format data model keyed on `uuid`, and a genuine
  from-scratch TCP broker rather than wrapping a framework.
- **Correctness bugs.** A SQL type typo (`BIGGABLE` → `BIGINT`) and an unused test helper
  that broke the build — both caught by the human through build validation.
- **A flaky test.** The consumer-group load-sharing test was timing-dependent; the human
  diagnosed it and fixed it by subscribing both consumers and letting the rebalance settle
  before publishing, then stress-tested it.
- **Environment and toolchain friction.** Go toolchain alignment with `golang:1.25`, image
  compatibility, `PATH` issues, and local Kubernetes setup all needed human steering.
- **A design boundary found only in validation.** `kubectl scale` leaves a new streamer
  idle because its `REPLICAS` env is static, so streamers must be scaled via Helm —
  surfaced by the human probing behavior, then documented as a known boundary.
- **Over-engineering and verbosity.** AI drafts trended wordy and over-abstracted; repeated
  "keep it simple" / "is this too wordy?" direction was needed to keep the scope tight.

## Prompts That Materially Shaped The Project

The project began with an iterative design phase rather than implementation.

Several architectural alternatives were explored and challenged before code was written.

Some of the prompts that most influenced the final result are listed below.

### Core Architecture

- **"Build an elastic, scalable telemetry pipeline around a custom message queue — no Kafka, RabbitMQ, or ZeroMQ."** → the from-scratch TCP broker as the centrepiece rather than a library.
- **"Design the queue to scale, preserve ordering, and stay reliable without reinventing Kafka."** → partitions for parallelism, per-GPU ordering, at-least-once — deliberately minimal.
- **"How do we avoid duplicate telemetry under at-least-once delivery?"** → idempotent writes via a unique `(uuid, metric, ts)` constraint plus persist-then-ack.
- **"PostgreSQL, TimescaleDB, Cassandra, InfluxDB, or MongoDB?"** → the database comparison and the PostgreSQL choice, with TimescaleDB as an upgrade path.
- **"Store DCGM as long-format generic samples keyed by UUID rather than host-local GPU identifiers."** → the long-format data model, safe against cross-host id collisions.

### Reliability and Failure Handling

- **"What failure scenarios must be tested before this implementation is complete?"**
- **"How do I verify that rebalancing is actually happening?"**
- **"Does the database grow unbounded, and what happens when it does?"**
- **"Document broker failure semantics and delivery guarantees explicitly."**

### Developer Experience and Operations

- **"Cover approximately 90% of the logic with tests while keeping the code simple and readable."**
- **"Refine every component to follow the same conventions."**
- **"Provide a way to start and stop telemetry generation during demonstrations."**
- **"Add Swagger support so the API can be explored interactively."**
- **"If a reviewer blindly follows the README, will the project actually run?"**

### Engineering Standards and Project Structure

- **"Use logrus instead of slog so log level and format are configurable through the Kubernetes ConfigMap."**
- **"Use a single Go module rather than multiple modules."**
- **"Keep one generic root Makefile rather than per-component Makefiles."**
- **"Put configuration and the logger in an internal/config package, keep cmd/main.go thin, and don't unit-test main."**

### Validation and Review

- **"Give me a step-by-step system validation process."**
- **"What aspects of this design do I need to understand deeply?"**
- **"Is the message queue design actually good?"**
- **"Does this resemble an existing message queue implementation?"**
- **"How do I know if the queue buffer actually contains data?"**
- **"What changes would make this design stronger?"**

These discussions transformed AI from a code generator into an architectural reviewer and adversarial reviewer that challenged assumptions and improved the final design.

## Verification (Human-Owned)

All correctness claims were validated by the human rather than accepted from AI output.

Validation activities included:

- `go build ./...`
- `go vet ./...`
- `go test ./...`

Additionally, the entire system was validated end-to-end on a live Kubernetes cluster using Kind.

Validation scenarios included:

- Telemetry ingestion
- PostgreSQL persistence
- API functionality
- Swagger integration
- Consumer group rebalancing
- Collector failover
- At-least-once redelivery
- Backpressure behavior
- Broker restart recovery

Detailed validation steps and observations are documented in `VALIDATION.md`.

## Honest Assessment

AI dramatically accelerated implementation by generating boilerplate code, tests, documentation, and deployment assets that would otherwise have taken considerably longer to produce.

However, it functioned as an accelerator rather than an autopilot.

The human remained responsible for:

- Setting requirements
- Making architectural decisions
- Identifying incorrect assumptions
- Catching implementation defects
- Controlling complexity
- Verifying correctness
- Demonstrating the system under realistic failure conditions

The final result reflects that partnership rather than either side working independently.
