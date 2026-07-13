# syntax=docker/dockerfile:1
# Database image: stock PostgreSQL with the telemetry schema baked into the
# init directory. On first start Postgres runs everything in
# /docker-entrypoint-initdb.d, creating the gpu_samples table + indexes.
FROM postgres:16-alpine
COPY database/init/ /docker-entrypoint-initdb.d/
