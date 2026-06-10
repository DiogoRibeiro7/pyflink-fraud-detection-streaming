# Iceberg Sink Design

This repository now includes a typed sink abstraction layer for two record families:

- `TransactionSink` for validated raw transactions
- `AlertSink` for emitted fraud alerts

The runnable local fallback is file-based. The Iceberg path is intentionally documented as an extension point rather than presented as a fully production-ready local demo.

## What works now

For the local CLI (`fraud-local`), you can route records to:

- `stdout` for alerts
- JSONL files for alerts and transactions
- Parquet files for alerts and transactions when `pyarrow` is installed

Example:

```bash
poetry run fraud-local data/sample_transactions.jsonl \
  --show-all \
  --alert-sink jsonl \
  --alert-output artifacts/alerts.jsonl \
  --transaction-sink jsonl \
  --transaction-output artifacts/transactions.jsonl
```

Parquet example:

```bash
pip install pyarrow
poetry run fraud-local data/sample_transactions.jsonl \
  --show-all \
  --alert-sink parquet \
  --alert-output artifacts/alerts.parquet
```

## Iceberg integration status

The codebase includes `src/fraud_streaming/sinks/iceberg.py` with:

- an `IcebergSinkConfig`
- `IcebergTransactionSink`
- `IcebergAlertSink`

Those classes validate optional dependencies and provide the shape for future append logic, but they do not pretend to offer a fully validated end-to-end Iceberg writer in this repository yet.

Current behaviour:

- if `pyiceberg` is missing, the CLI fails with a clear installation error
- if `pyiceberg` is installed, the sink still raises a clear runtime message explaining that append logic must be completed for the target catalog

This is deliberate. Iceberg environments vary significantly across catalogs, credentials, and object stores, so a fake "works everywhere" implementation would be misleading.

## Local Docker-style demo path

For a local portfolio demo, the simplest coherent path is:

1. Run the local fraud pipeline.
2. Persist validated transactions and alerts to JSONL or Parquet.
3. Use those files as the source for an external Iceberg ingestion step.

If you want a fuller local Iceberg setup, the usual moving parts are:

- S3-compatible object storage such as MinIO
- a catalog such as REST, Hive Metastore, or Nessie
- `pyiceberg` or Spark/Flink-based writers
- explicit schema management for alert and transaction rows

This repository does not bundle those services because the local developer experience would become much heavier and the ordinary unit test path would no longer stay lightweight.

## AWS Glue Catalog + S3 deployment-style path

For a more production-shaped setup, a typical target architecture is:

- S3 bucket for raw transactions and alert tables
- AWS Glue Catalog for Iceberg table metadata
- writers running from Flink, Spark, or a controlled batch job
- IAM roles scoped to the relevant Glue and S3 resources

Important considerations:

- keep transaction and alert schemas versioned and explicit
- decide whether low-risk records are stored together with elevated alerts or separated
- define partitioning carefully, for example by event date and possibly risk level
- treat analyst feedback and retraining exports as separate tables rather than overloading the alert table

## Local CLI flags

`fraud-local` supports these sink-related options:

- `--alert-sink stdout|jsonl|parquet|iceberg`
- `--alert-output PATH`
- `--transaction-sink none|jsonl|parquet|iceberg`
- `--transaction-output PATH`
- `--iceberg-catalog-uri`
- `--iceberg-warehouse`
- `--iceberg-alert-table`
- `--iceberg-transaction-table`

Validation rules:

- file sinks require an output path
- Iceberg sinks require catalog URI, warehouse, and table name
- ordinary local usage still defaults to stdout alerts and no transaction sink

## Limitations

- No bundled `pyiceberg` dependency
- No catalog bootstrap automation
- No authenticated object-store example in code
- No PyFlink Iceberg sink implementation in the runtime wrapper yet

That limitation is intentional and should be called out in any portfolio walkthrough.
