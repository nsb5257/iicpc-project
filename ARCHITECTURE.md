# IICPC Platform Architecture

This document provides a comprehensive overview of the IICPC distributed benchmarking platform. The system is designed to securely sandbox contestant code, subject it to high-throughput synthetic trading loads, validate its correctness in real-time, and broadcast the resulting percentiles to a live leaderboard.

---

## 1. System Components (Microservices)

The platform is divided into four distinct Go microservices, prioritizing separation of concerns and independent scalability.

### A. Sandbox Service (`cmd/sandbox`)
* **Role:** The secure execution engine.
* **Flow:** 1. Accepts code uploads (Go, C++, Rust) via HTTP multipart forms.
  2. Dynamically generates a language-specific Dockerfile and builds a temporary image.
  3. Spawns an isolated container from the image.
  4. Exposes an internal gRPC API (`ExecuteSubmission`) to coordinate container lifecycles.
* **Key Features:** Background janitor for cleaning up stale images, deterministic host port routing, and strict resource limits.

### B. Fleet Service (`cmd/fleet`)
* **Role:** The distributed load generator.
* **Flow:** 1. Idles until triggered by a `POST /run` request containing target parameters.
  2. Spawns a highly concurrent pool of goroutines (bots) constrained by a Token Bucket algorithm to strictly enforce target TPS.
  3. Generates diverse, randomized orders (Type, Side, Price, Quantity).
  4. Captures the exact HTTP response body from the contestant's trading engine.
  5. Publishes raw telemetry (`OrderEvent` JSON) to a Redpanda message queue.

### C. Telemetry Service (`cmd/telemetry`)
* **Role:** The data ingester and correctness validator.
* **Flow:** 1. Continuously consumes telemetry events from Redpanda.
  2. Validates the JSON response from the contestant's container against strict trading invariants (e.g., `filled_qty <= ordered_qty` and LIMIT price boundaries).
  3. Enriches the event with an `is_correct` boolean.
  4. Uses the PostgreSQL `COPY` protocol to batch-insert thousands of rows per second into TimescaleDB.

### D. Leaderboard Service (`cmd/leaderboard`)
* **Role:** The real-time scoring engine and UI broadcaster.
* **Flow:** 1. Periodically queries TimescaleDB, grouping by `submission_id`.
  2. Computes overall TPS and exact latency percentiles (P50, P90, P99) *only* for successful and correct orders.
  3. Calculates a composite score `(TPS * 100) / P90` and ranks the submissions.
  4. Publishes the ranked JSON array to Redis Pub/Sub, which is immediately pushed to connected browsers via WebSockets.

---

## 2. Protocols & Communication

To optimize performance and reliability, the platform utilizes several communication protocols tailored to specific tasks:

* **HTTP/REST:** Used for external ingress (uploading source code, triggering the Fleet load test via `/run`, and serving health/readiness probes).
* **gRPC:** Used internally between the Sandbox Service and the orchestrator. Protocol buffers ensure strict typing and rapid serialization when spawning containers.
* **Kafka Protocol (Redpanda):** Provides highly durable, asynchronous streaming of telemetry between the Fleet bots and the Telemetry ingester, preventing data loss during traffic spikes.
* **Redis Pub/Sub:** Serves as a lightweight message broker to fan-out leaderboard state changes from the scoring engine to multiple WebSocket handlers.
* **WebSockets:** Maintains persistent, bi-directional connections to the browser clients (`index.html`), allowing the UI to re-render in real-time without polling.

---

## 3. Data Stores

* **Redpanda (Message Queue):** A Kafka-compatible streaming platform. It acts as a shock absorber. If the database slows down, Redpanda buffers the events on disk (persisted via a 5Gi PVC) until the Telemetry service catches up. Configured with 4 partitions to allow concurrent ingestion.
* **TimescaleDB (Time-Series Database):** A PostgreSQL extension optimized for time-series data. It stores the `order_metrics` hypertable (persisted via a 10Gi PVC). TimescaleDB was chosen for its native capability to calculate complex percentiles (`percentile_cont`) over massive datasets efficiently.
* **Redis (In-Memory Cache):** Used ephemerally for Pub/Sub event broadcasting. No persistent storage is required here as the Leaderboard's state is fully reconstructable from TimescaleDB.

---

## 4. Isolation & Security Strategy

Running untrusted contestant code requires paranoid security configurations. The Sandbox implements the following multi-layered isolation:

1. **Memory & CPU Pinning:** Containers are hard-capped at 128MB of RAM (`Memory: 128 << 20`) and pinned to a single CPU core (`CpusetCpus: "0"`). This prevents noisy neighbors or intentional fork bombs from taking down the host.
2. **Internal Bridge Network:** On startup, the Sandbox generates a dedicated Docker bridge network (`iicpc-sandbox-net`) configured with `Internal: true`. This provides genuine network isolation by silently dropping all internet egress traffic, preventing contestant code from reaching external endpoints. 
3. **Deterministic Port Routing:** To preserve the Fleet-to-Container traffic path while isolated, the container's execution port is explicitly mapped to a deterministic host port. This port is dynamically calculated by hashing the `submission_id` modulo a 30000–40000 range. This routing methodology entirely eliminates slow Docker container inspection commands and concurrency race conditions.
4. **Ephemeral Lifecycles:** Containers are built with `AutoRemove: true` so they self-destruct upon exit, and an automated janitor sweeps the Docker daemon every 30 minutes to aggressively delete old images.
5. **Memory Exhaustion Protection:** The Fleet explicitly truncates contestant HTTP responses to 2KB using `io.LimitReader` to protect the load generators from maliciously infinite responses.

---

## 5. Scaling Strategy

* **Horizontal Pod Autoscaling (HPA):** The `Fleet` and `Telemetry` services are entirely stateless. As target TPS requirements grow, Kubernetes can horizontally scale these pods. Redpanda's partitioned topics allow multiple Telemetry pods to consume messages concurrently without overlapping.
* **Database Write Optimization:** Instead of row-by-row inserts, the Telemetry service queues data in memory and flushes it using PostgreSQL's bulk `COPY` command. It utilizes a hybrid Time/Size batching algorithm (e.g., flush every 1,000 records OR every 5 seconds) to maximize throughput while preventing stale data.
* **Read-Optimized Scoring:** The Leaderboard does not execute heavy analytical queries on every client request. It queries the database precisely once per tick (2 seconds) centrally, ranks the array, and streams the pre-computed result to N WebSockets, making UI scaling effectively free.