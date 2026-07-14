# AI Usage

This project was built with extensive use of AI coding assistant (GitHub Copilot, Claude). This document provides an honest account of how AI was used, where it accelerated development, where it fell short, and where human judgment and intervention were required.

The goal is not to claim more or less AI usage than actually occurred, but to clearly communicate the division of labor between the AI tools and the engineer responsible for the system.

---

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

---

## Where AI Was Used

### Design

AI was used heavily during the architecture phase to explore and compare design alternatives before implementation began.

Examples include:

- Message queue architecture
- Partitioning strategies
- Delivery guarantees
- Database selection
- Consumer group design
- Failure handling
- Backpressure mechanisms
- Kubernetes deployment models

AI produced strong initial drafts of the design documentation and architectural alternatives, while the human refined structure, simplified unnecessary complexity, and ensured the design matched the actual implementation.

---

### Implementation

AI assisted in implementing all five major components:

- Telemetry Streamer
- Custom Message Queue Broker
- Telemetry Collector
- API Gateway
- Database assets and migrations

Examples of AI-generated implementation work include:

- Length-prefixed TCP wire protocol
- Partition and consumer group logic
- Offset tracking and acknowledgement flow
- Backpressure handling
- PostgreSQL persistence layer using pgx
- REST API handlers
- OpenAPI generation

---

### Testing

AI generated the majority of the table-driven unit tests across the project.

Coverage focused primarily on business logic packages rather than entrypoints or boilerplate code.

Examples include:

- Consumer group logic
- Partition ownership
- Wire protocol serialization
- CSV parsing
- Pipeline orchestration
- API handlers

The human defined:

- Coverage targets (~90% for logic packages)
- Package boundaries
- Testing conventions
- Integration test strategy

---

### Operations and Deployment

AI assisted with:

- Multi-stage Dockerfiles
- Distroless container images
- Helm charts
- Kind cluster configuration
- Makefile targets
- OpenAPI generation
- Swagger UI integration

---

### Documentation

AI contributed significantly to:

- README
- DESIGN document
- AI usage documentation
- Validation runbooks
- Swagger integration

---

### Explanation and Review

A significant portion of the collaboration involved AI explaining its own output.

Examples include:

- Partitioning math
- Consumer group semantics
- Offset handling
- StatefulSet versus Deployment
- Delivery guarantees

This acted as a review mechanism because explaining a design often exposes weaknesses or incorrect assumptions.

---

## Where AI Fell Short

### Architectural Decisions Were Human-Owned

AI implemented the designs it was asked to build but did not independently drive the architectural decisions that shaped the project.

Examples of human-owned decisions include:

- Single Go module rather than multiple modules
- Single root Makefile rather than component-level Makefiles
- Long-format telemetry storage model
- Using GPU UUID as the primary key
- Building a genuine custom TCP broker rather than wrapping an existing framework

---

### Correctness Bugs

AI introduced several defects that required human intervention:

- SQL type typo (`BIGGABLE` instead of `BIGINT`)
- Unused test helper causing build failures
- Minor API inconsistencies

These were identified through build validation and manual review.

---

### Flaky Tests

A consumer-group load-sharing test failed intermittently because it relied on timing assumptions.

The issue was diagnosed manually and resolved by:

- Ensuring both consumers subscribed before publishing
- Allowing rebalance completion before assertions
- Stress testing the scenario repeatedly

---

### Environment and Toolchain Issues

Several problems required manual intervention:

- Go toolchain alignment with `golang:1.25`
- Docker image compatibility
- PATH configuration issues
- Local Kubernetes environment setup

---

### Design Boundaries Discovered During Validation

Live validation surfaced behaviors that AI had not proactively identified.

One example involved streamer scaling:

- Scaling via `kubectl scale` created new streamer pods.
- New pods remained idle because the `REPLICAS` environment variable was static.
- Correct scaling therefore required Helm upgrades rather than direct Kubernetes scaling.

This limitation was documented and added to future work.

---

### Over-Engineering and Verbosity

AI drafts frequently trended toward:

- Overly verbose documentation
- Excessive abstractions
- Unnecessary complexity

Repeated human guidance such as:

- "Keep it simple."
- "Is this too wordy?"
- "Do we actually need this?"

was required to keep the project appropriately scoped.

---

## Prompts That Materially Shaped The Project

The project began with an iterative design phase rather than implementation.

Several architectural alternatives were explored and challenged before code was written.

Some of the prompts that most influenced the final result are listed below.

### Core Architecture

- **"Build an elastic, scalable telemetry pipeline around a custom message queue — no Kafka, RabbitMQ, or ZeroMQ."**
- **"Design the queue to scale, preserve ordering, and stay reliable without reinventing Kafka."**
- **"How do we avoid duplicate telemetry under at-least-once delivery?"**
- **"PostgreSQL, TimescaleDB, Cassandra, InfluxDB, or MongoDB?"**
- **"Store DCGM as long-format generic samples keyed by UUID rather than host-local GPU identifiers."**

---

### Reliability and Failure Handling

- **"What failure scenarios must be tested before this implementation is complete?"**
- **"How do I verify that rebalancing is actually happening?"**
- **"Does the database grow unbounded, and what happens when it does?"**
- **"Document broker failure semantics and delivery guarantees explicitly."**

---

### Developer Experience and Operations

- **"Cover approximately 90% of the logic with tests while keeping the code simple and readable."**
- **"Refine every component to follow the same conventions."**
- **"Provide a way to start and stop telemetry generation during demonstrations."**
- **"Add Swagger support so the API can be explored interactively."**
- **"If a reviewer blindly follows the README, will the project actually run?"**

---

### Validation and Review

- **"Give me a step-by-step system validation process."**
- **"What aspects of this design do I need to understand deeply?"**
- **"Is the message queue design actually good?"**
- **"Does this resemble an existing message queue implementation?"**
- **"How do I know if the queue buffer actually contains data?"**
- **"What changes would make this design stronger?"**

These discussions transformed AI from a code generator into an architectural reviewer and adversarial reviewer that challenged assumptions and improved the final design.

---

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

---

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
