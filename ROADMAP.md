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

- [x] Add a transaction producer that streams generated events into Kafka.
- [x] Add a consumer that reads fraud alerts from Kafka.
- [x] Add a docker-compose profile with Kafka/Redpanda and a Flink cluster.
- [x] Add make targets for end-to-end local demos.
- [x] Add connector JAR configuration notes for each Flink version.

## Stage 3 — ML scoring

- [x] Train a baseline model using generated and/or public fraud data.
- [x] Add model artifact loading.
- [x] Combine model score with rule-based risk explanations.
- [x] Add feature parity checks between offline training and streaming inference.
- [x] Add calibration metrics and threshold analysis.

## Stage 4 — Lakehouse and observability

- [x] Write raw transactions and alerts to Apache Iceberg.
- [x] Add event quality checks.
- [x] Add Prometheus-friendly metrics.
- [x] Add dead-letter handling for malformed events.
- [x] Add replay support for historical streams.

## Stage 5 — Data science monitoring

- [x] Track fraud score distribution drift.
- [x] Track feature drift by user segment, country, merchant category, and channel.
- [x] Add alert precision review tables.
- [x] Add analyst feedback ingestion.
- [x] Add retraining dataset export.

## Stage 6 — Production hardening

- [x] Add CI with pytest, ruff, and mypy.
- [x] Add Docker image publishing.
- [x] Add infrastructure examples for AWS MSK / Kinesis / Managed Service for Apache Flink.
- [x] Add checkpointing and savepoint operational documentation.
- [x] Add load tests and throughput benchmarks.

## Stage 7 — Stretch ideas

- [x] Evaluate on a public or organization-provided labelled fraud dataset with clear provenance notes.
- [x] Add event-time watermarks and explicit late-event behaviour in the PyFlink wrapper.
- [x] Add state compatibility fixtures for upgrade testing across saved `UserProfileState` payload versions.
- [x] Add a lightweight analyst review UI on top of the feedback ingestion flow.
- [ ] Add end-to-end managed-cloud deployment validation in a real AWS environment.
