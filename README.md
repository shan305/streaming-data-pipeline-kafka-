# Crypto Market Data Streaming Pipeline

A real-time cryptocurrency market data pipeline demonstrating system boundaries, durability, replay semantics, and correctness under failure.

## Overview

This project implements a streaming pipeline that ingests live trade data from Binance, processes it through Kafka, and routes it into hot and cold storage paths.

The goal is not to build an application or trading bot. The goal is to demonstrate system design thinking: clear data boundaries, replayable streams, idempotent consumers, separation of latency-sensitive and durable paths, and explicit handling of failure modes.

## Architecture

```
Binance WebSocket
       |
       v
Ingestion Producer
       |
       v
Kafka (Redpanda)
       |
       +---------------------+
       |                     |
       v                     v
Hot Cache Consumer      Cold Store Consumer
(Redis)                 (Postgres)
       |
       v
Read-only API
```

## Why Kafka

Kafka is not used for scale or buzzwords. It exists as a durability and replay boundary.

Producers may disconnect. Consumers may crash or lag. Storage writes may partially fail. Kafka allows the system to replay data deterministically, decouple ingestion from storage, and reason about correctness under failure.

Without Kafka, this would be a script.

## Data Flow

### Ingestion (Producer)

Connects to Binance WebSocket streams, normalizes trade events, and produces messages to Kafka with deterministic keys.

Key format: `<symbol>-<event_time_ms>`

This enables idempotent consumption, safe reprocessing, and duplicate tolerance.

### Hot Path (Redis)

Purpose: Low-latency access to latest prices.

Characteristics: Overwrite-based, TTL-controlled, not replayed, no historical guarantees. This path is allowed to lose data.

### Cold Path (Postgres)

Purpose: Durable historical storage.

Characteristics: Append-only, idempotent inserts, replay-safe via unique constraint on `(symbol, event_time_ms)`. If Kafka replays data, duplicates are safely ignored.

## Correctness Guarantees

### Idempotency

Kafka message keys are deterministic. The database enforces uniqueness. Consumers tolerate duplicate delivery.

### Replay Safety

Consumers use manual offset commits. Offset reset is set to earliest. The cold store can be rebuilt from Kafka.

### Failure Handling

Explicitly handled: WebSocket disconnects, consumer restarts, duplicate messages, partial database writes.

Not handled (by design): Exactly-once semantics, cross-region replication, schema evolution.

## Observability

The system includes lightweight observability without external tooling: message counters, processing lag tracking, and warning thresholds for lag spikes.

Example log output:

```
High lag for btcusdt: 5400ms
```

This demonstrates lag awareness, consumer health visibility, and backpressure detection.

## What This Project Is Not

This project intentionally avoids UI, dashboards, trading logic, authentication, Kubernetes, microservice sprawl, and enterprise abstractions. Adding those would weaken the signal.

## Running the Project

### Infrastructure

```bash
docker compose up -d
```

Services: Redpanda (Kafka), Redis, Postgres, Redpanda Console.

### Python Services

```bash
python -m ingestion.producer
python -m consumers.hot_cache
python -m consumers.cold_store
python -m api.server
```

### Verify Hot Path

```bash
curl http://localhost:5000/price/btcusdt
```

### Verify Cold Path

```sql
SELECT COUNT(*) FROM trades;
SELECT * FROM trades ORDER BY event_time_ms DESC LIMIT 5;
```

## Design Tradeoffs

| Decision | Tradeoff |
|----------|----------|
| Kafka | Operational complexity for replay and correctness |
| Redis hot path | Speed over durability |
| Postgres cold path | Durability over latency |
| Manual offsets | More code, clearer semantics |
| No UI | Stronger systems signal |

## Project Scope

This project is complete.

Future additions should be limited to documentation and explanation. Adding a frontend, more consumers, orchestration, or premature optimization would be overbuilding.

## Purpose

This project demonstrates the systems

It answers: Where does data become durable? What happens if something crashes? How can the system be replayed? What data can be lost, and what cannot?

## License

MIT