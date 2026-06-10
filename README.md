# PyFlink Fraud Detection Streaming

A portfolio-grade streaming system for suspicious card transaction detection with **PyFlink**, stateful feature engineering, and explainable risk scoring.

It mirrors a production-style flow while keeping the fraud logic in normal, testable Python:

```text
Kafka transactions
        ↓
PyFlink streaming job
        ↓
stateful per-user features
        ↓
risk scoring and alert generation
        ↓
Kafka / stdout / downstream store
```

## Highlights

- Pure-Python fraud logic separated from Flink runtime wiring
- Stateful per-user and per-card features for real-time scoring
- Explainable alerts with human-readable fraud reasons
- Local runner and tests that do not require Flink or Kafka
- Clear extension path for Kafka, Iceberg, and ML scoring

## Why this project is useful

Fraud detection is a strong PyFlink use case because it needs:

- low-latency decisions as transactions arrive;
- keyed state per user, card, or account;
- rolling-window behaviour such as transaction velocity;
- late-event and event-time thinking;
- explainable alerts that can be reviewed by analysts.

Apache Flink is built for stateful computations over bounded and unbounded streams, and its Python DataStream API supports stream transformations such as filtering, state updates, windows, and aggregation.

## Repository layout

```text
.
├── src/fraud_streaming/
│   ├── schemas.py          # Typed transaction, feature, state, and alert models
│   ├── features.py         # Stateful feature engineering
│   ├── rules.py            # Explainable fraud scoring rules
│   ├── serialization.py    # JSON parsing and alert serialization
│   ├── local_runner.py     # Local non-Flink runner for development and tests
│   ├── flink_job.py        # PyFlink DataStream job
│   └── cli.py              # Command-line entrypoints
├── scripts/
│   └── generate_transactions.py
├── tests/
├── data/
│   └── sample_transactions.jsonl
├── docker-compose.yml
├── Dockerfile
└── ROADMAP.md
```

## Quick start without Flink

This mode is useful for reviewing the project and testing the fraud logic.

```bash
poetry install --with dev
make quality
poetry run pytest
poetry run fraud-local data/sample_transactions.jsonl
```

Or with plain Python:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e . pytest
pytest
python -m fraud_streaming.cli data/sample_transactions.jsonl
```

Useful local shortcuts:

```bash
make quality
make test
make local-demo
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the local development workflow and optional extras.

## Generate synthetic transactions

```bash
python scripts/generate_transactions.py --output data/generated_transactions.jsonl --users 20 --transactions 500 --seed 7
python -m fraud_streaming.cli data/generated_transactions.jsonl --show-all
```

## Produce transactions to Kafka

Install the optional Kafka extra:

```bash
poetry install --with dev -E kafka
```

Publish existing JSONL events:

```bash
poetry run fraud-produce-transactions \
  --bootstrap-servers localhost:9092 \
  --topic transactions \
  --input data/sample_transactions.jsonl
```

Or generate and stream synthetic events progressively:

```bash
poetry run fraud-produce-transactions \
  --bootstrap-servers localhost:9092 \
  --topic transactions \
  --users 20 \
  --transactions 500 \
  --seed 7 \
  --sleep-ms 100 \
  --key-field user_id
```

The producer validates each event with the same canonical transaction schema used by the local runner and the Flink wrapper.

## Consume fraud alerts from Kafka

With the Kafka extra installed, you can inspect the alert stream produced by the Flink job:

```bash
poetry run fraud-consume-alerts \
  --bootstrap-servers localhost:9092 \
  --topic fraud-alerts \
  --group-id fraud-demo \
  --from-beginning \
  --max-messages 20 \
  --summary
```

Optional filters:

```bash
poetry run fraud-consume-alerts \
  --bootstrap-servers localhost:9092 \
  --topic fraud-alerts \
  --risk-level high \
  --min-risk-score 70
```

## Run the PyFlink job locally

Install the optional Flink dependency first:

```bash
poetry install --with dev -E flink
```

Then run against a local JSONL file source:

```bash
poetry run pyflink-fraud-job \
  --source file \
  --input data/sample_transactions.jsonl \
  --sink stdout
```

For Kafka:

```bash
poetry run pyflink-fraud-job \
  --source kafka \
  --bootstrap-servers localhost:9092 \
  --input-topic transactions \
  --output-topic fraud-alerts
```

The Kafka path assumes the required Flink Kafka connector is available to the Flink runtime. In managed Flink environments this is normally configured at the cluster/job level.

Connector setup notes and troubleshooting live in [`docs/flink-kafka-connectors.md`](docs/flink-kafka-connectors.md).

## Docker Compose streaming demo

The repository includes a local streaming demo profile with:

- Redpanda as the Kafka-compatible broker
- a standalone Flink JobManager and TaskManager
- optional producer and consumer containers

Bring up the broker and Flink services:

```bash
make compose-up
```

Seed the `transactions` topic:

```bash
make produce-demo
```

Submit the Kafka-to-Kafka PyFlink job:

```bash
make run-flink-kafka-demo
```

Read alerts back from Kafka:

```bash
make consume-alerts
```

Tear the demo down:

```bash
make compose-down
```

Important connector note:

- The compose demo does not bundle the Flink Kafka connector JAR automatically.
- Place a Flink `1.19.x` compatible Kafka connector JAR under [`docker/flink/connectors`](docker/flink/connectors/README.md) before running the full Kafka/Flink path.

Validation status:

- `docker compose config` was used as the configuration-level validation target in the agent environment.
- Full end-to-end execution with a real connector JAR still needs manual verification on a machine with Docker available.

## Example input event

```json
{
  "transaction_id": "tx-000001",
  "user_id": "user-001",
  "card_id": "card-001",
  "merchant_id": "merchant-042",
  "amount": 42.50,
  "currency": "EUR",
  "country": "PT",
  "device_id": "device-001",
  "merchant_category": "grocery",
  "event_time": "2026-06-10T12:00:00Z",
  "channel": "pos",
  "is_card_present": true
}
```

## Example alert

```json
{
  "transaction_id": "tx-000020",
  "user_id": "user-001",
  "card_id": "card-001",
  "event_time": "2026-06-10T12:07:30+00:00",
  "risk_score": 85,
  "risk_level": "high",
  "reasons": [
    "transaction velocity is high",
    "amount is unusual for the user",
    "country changed recently"
  ]
}
```

## Dead-letter handling for malformed events

The local runner can keep processing valid events while writing malformed rows to a dead-letter JSONL file:

```bash
poetry run fraud-local data/sample_transactions.jsonl \
  --show-all \
  --dead-letter-output data/dead_letter.jsonl
```

Dead-letter records preserve:

- the raw event payload
- the parse error, if JSON decoding failed
- structured quality check failures
- the ingestion timestamp

Current quality checks cover missing required fields, invalid amounts, invalid timestamps, unsupported currencies, empty user/card identifiers, future event timestamps beyond tolerance, and duplicate transaction IDs within a local batch.

## Local metrics export

The local runner can also write Prometheus text format metrics:

```bash
poetry run fraud-local data/sample_transactions.jsonl \
  --show-all \
  --metrics-output data/fraud_metrics.prom
```

The exported metrics include:

- processed transactions
- emitted alerts
- high-risk alerts
- malformed events
- average risk score
- event counts by country, channel, and risk level

Example output:

```text
# HELP fraud_transactions_processed_total Total number of successfully processed transactions.
# TYPE fraud_transactions_processed_total counter
fraud_transactions_processed_total 8
# HELP fraud_average_risk_score Average risk score of successfully processed transactions.
# TYPE fraud_average_risk_score gauge
fraud_average_risk_score 18.125
```

## Offline ML training baseline

The repository now includes an optional offline training pipeline that reuses the same streaming-style feature logic used during local processing.

Install the ML extra:

```bash
poetry install --with dev -E ml
```

Train on sample or synthetic data:

```bash
poetry run fraud-train-model --input data/sample_transactions.jsonl
```

Or generate a larger synthetic training set on the fly:

```bash
poetry run fraud-train-model \
  --users 50 \
  --transactions 2000 \
  --seed 7 \
  --output-dir artifacts
```

Each run writes a versioned artifact directory containing:

- `model.pkl`
- `feature_schema.json`
- `metrics.json`

The saved metrics include precision, recall, F1, ROC-AUC when the evaluation split contains both classes, and a threshold analysis table.

Important label note:

- If the input data includes a `label` field, it must be binary `0/1` or boolean.
- If no label is present, the pipeline creates synthetic demo labels from transparent heuristics so the ML workflow can be demonstrated.
- Those synthetic labels are not real fraud ground truth and should not be presented as such.

## Model scoring during inference

Rule-only scoring remains the default. To enable model-aware scoring after training a model artifact:

```bash
poetry run fraud-local data/sample_transactions.jsonl \
  --scoring-strategy blend \
  --model-artifact artifacts/model-YYYYMMDDTHHMMSSZ/model.pkl
```

Available strategies:

- `rules`: existing explainable rule score only
- `model`: model probability scaled to `0-100`, while still attaching rule reasons
- `blend`: weighted average of rule score and model score

You can control the blend:

```bash
poetry run fraud-local data/sample_transactions.jsonl \
  --scoring-strategy blend \
  --model-artifact artifacts/model-YYYYMMDDTHHMMSSZ/model.pkl \
  --rule-weight 0.7 \
  --model-weight 0.3
```

Model artifacts fail fast if the stored raw feature schema does not match the canonical inference feature schema.

## Feature parity checks

To check offline-versus-inference feature parity on deterministic JSONL inputs:

```bash
poetry run fraud-check-feature-parity \
  --reference data/sample_transactions.jsonl \
  --current data/sample_transactions.jsonl \
  --output artifacts/feature_parity_report.json \
  --deterministic
```

The parity report checks:

- feature column presence
- feature column order against the canonical schema
- coarse dtype compatibility
- null-rate differences
- row-count agreement
- deterministic value differences when `--deterministic` is set

The command exits with a non-zero status when parity fails and always saves a JSON report.

## Replay historical streams

Replay historical JSONL transactions to stdout:

```bash
poetry run fraud-replay data/sample_transactions.jsonl --as-fast-as-possible
```

Preserve event-time spacing:

```bash
poetry run fraud-replay data/sample_transactions.jsonl --preserve-timing
```

Replay faster than wall-clock event spacing and filter the slice:

```bash
poetry run fraud-replay data/sample_transactions.jsonl \
  --speed-multiplier 10 \
  --start-time 2026-06-10T12:00:00Z \
  --end-time 2026-06-10T13:00:00Z \
  --user-id user-001
```

Dry-run the replay plan without emitting:

```bash
poetry run fraud-replay data/sample_transactions.jsonl --dry-run --speed-multiplier 20
```

You can also replay to a file or Kafka:

```bash
poetry run fraud-replay data/sample_transactions.jsonl --output-mode file --output-path data/replayed.jsonl
poetry run fraud-replay data/sample_transactions.jsonl --output-mode kafka --bootstrap-servers localhost:9092 --topic replay-transactions
```

## Core features

For each user/card stream, the project computes:

- transaction count in the last 5 minutes;
- transaction amount in the last 1 hour;
- minutes since previous transaction;
- amount z-score against user history;
- country change flag;
- device change flag;
- card-not-present flag;
- night-time transaction flag.

The default scoring is rule-based for transparency. This is intentional for a portfolio project: it shows the pipeline clearly before adding ML models.

## Production extensions

See [`ROADMAP.md`](ROADMAP.md) for the next stages: model scoring, feature store integration, Iceberg/S3 sink, drift monitoring, and CI/CD.
