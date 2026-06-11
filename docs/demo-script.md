# 5-Minute Demo Script

This walkthrough is designed for a recruiter, hiring manager, or senior engineer who wants to understand the project quickly without setting up Kafka or Flink first.

## 1. Start with the project story

Open the README and summarize the goal in one sentence:

> This project shows a stateful, explainable fraud detection pipeline in PyFlink, while keeping the fraud logic testable in normal Python.

Point out:

- pure Python fraud logic
- keyed state and rolling features
- explainable rule reasons
- optional Kafka, ML, Iceberg, AWS, and benchmark surfaces

## 2. Show the local path first

Run:

```bash
poetry install --with dev
make quality
poetry run python -c "import sys; sys.path.insert(0, 'src'); from fraud_streaming.cli import main; raise SystemExit(main(['data/sample_transactions.jsonl', '--show-all']))"
```

Talking points:

- no Kafka or Flink required
- same core feature logic as the streaming job
- deterministic sample data makes the behaviour easy to explain

## 3. Explain the interesting alert

Call out the suspicious event in the sample stream:

- transaction velocity becomes high
- amount is unusual relative to the user’s running history
- country changes quickly
- device changes
- card-not-present high-value behaviour appears

Use the checked-in sample files:

- local runner output: [`docs/samples/local_runner_output.jsonl`](samples/local_runner_output.jsonl)
- example alert: [`docs/samples/high_risk_alert.json`](samples/high_risk_alert.json)

## 4. Show operational surfaces

Point to:

- dead-letter handling
- Prometheus-style metrics
- replay support
- drift monitoring
- analyst feedback ingestion
- benchmarks

Useful artifacts:

- metrics sample: [`docs/samples/metrics_example.prom`](samples/metrics_example.prom)
- drift sample: [`docs/samples/drift_report_example.md`](samples/drift_report_example.md)

## 5. Show the streaming extension path

Explain that the local path is the review-friendly entry point, then point to:

- Kafka producer and consumer CLIs
- Docker Compose demo
- PyFlink wrapper
- connector notes
- checkpoint and savepoint operations guide

Suggested references:

- [`docs/flink-kafka-connectors.md`](flink-kafka-connectors.md)
- [`docs/flink-operations.md`](flink-operations.md)

## 6. Show the portfolio depth

Call out that the repository also includes:

- optional baseline ML training
- model/rule score blending
- feature parity checks
- AWS deployment templates
- Docker publishing workflow
- GitHub Actions CI

## 7. Close with trade-offs

End with the honest framing:

- the repository is strong on stateful feature engineering, explainability, and operational clarity
- it is intentionally cautious about pretending synthetic labels are real ground truth
- the heaviest production paths are documented as templates when they are not fully validated in-repo
