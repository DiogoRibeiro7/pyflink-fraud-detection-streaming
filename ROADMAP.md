# Roadmap

## Stage 1 — Core streaming baseline

- [x] Define typed transaction and alert schemas.
- [x] Implement stateful feature engineering in pure Python.
- [x] Implement explainable fraud scoring rules.
- [x] Add a local runner for deterministic development.
- [x] Add a PyFlink DataStream wrapper.
- [x] Add synthetic transaction generation.
- [x] Add tests for parsing, features, and rules.

## Stage 2 — Kafka-first demo

- [ ] Add a transaction producer that streams generated events into Kafka.
- [ ] Add a consumer that reads fraud alerts from Kafka.
- [ ] Add a docker-compose profile with Kafka/Redpanda and a Flink cluster.
- [ ] Add make targets for end-to-end local demos.
- [ ] Add connector JAR configuration notes for each Flink version.

## Stage 3 — ML scoring

- [ ] Train a baseline model using generated and/or public fraud data.
- [ ] Add model artifact loading.
- [ ] Combine model score with rule-based risk explanations.
- [ ] Add feature parity checks between offline training and streaming inference.
- [ ] Add calibration metrics and threshold analysis.

## Stage 4 — Lakehouse and observability

- [ ] Write raw transactions and alerts to Apache Iceberg.
- [ ] Add event quality checks.
- [ ] Add Prometheus-friendly metrics.
- [ ] Add dead-letter handling for malformed events.
- [ ] Add replay support for historical streams.

## Stage 5 — Data science monitoring

- [ ] Track fraud score distribution drift.
- [ ] Track feature drift by user segment, country, merchant category, and channel.
- [ ] Add alert precision review tables.
- [ ] Add analyst feedback ingestion.
- [ ] Add retraining dataset export.

## Stage 6 — Production hardening

- [ ] Add CI with pytest, ruff, and mypy.
- [ ] Add Docker image publishing.
- [ ] Add infrastructure examples for AWS MSK / Kinesis / Managed Service for Apache Flink.
- [ ] Add checkpointing and savepoint operational documentation.
- [ ] Add load tests and throughput benchmarks.
