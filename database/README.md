# Database Component

PostgreSQL instance backing the telemetry pipeline. This component ships no Go
code — it is a stock PostgreSQL image plus the schema in `init/01_schema.sql`,
which is the contract between the **collector** (writer) and **API gateway**
(reader).

- `init/01_schema.sql` — table + indexes, loaded on first start via the
  Postgres `docker-entrypoint-initdb.d` mechanism (mounted by the Helm chart).
- Deployed as a StatefulSet with a PersistentVolumeClaim (see
  `deployment/helm/database`).

The `gpu_samples` table stores DCGM long-format samples. The unique index on
`(uuid, metric, ts)` makes inserts idempotent so at-least-once delivery from the
queue does not create duplicates.
