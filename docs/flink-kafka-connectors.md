# Flink Kafka Connectors

## Why this matters

The PyFlink job wrapper in this repository can read from Kafka and write fraud alerts back to Kafka, but the Python package alone is not enough. The Flink runtime also needs a Kafka connector JAR that matches the Flink version in use.

The current project dependency targets:

| Project dependency | Supported range |
| --- | --- |
| `apache-flink` Python package | `>=1.19,<2.3` |

For local demos, keep the Python package version and the Flink runtime version aligned. A mismatch between the Python wheel, the JobManager/TaskManager runtime, and the Kafka connector JAR is one of the fastest ways to get confusing startup failures.

## Version matrix

Use a Kafka connector JAR that matches your Flink minor version:

| Flink runtime | Kafka connector guidance | Notes |
| --- | --- | --- |
| `1.19.x` | Use the Flink Kafka connector built for `1.19` | Good default target for local demos |
| `1.20.x` | Use the Flink Kafka connector built for `1.20` | Re-check package names and class availability |
| `2.0+` | Verify connector packaging before use | The project currently treats this as future-facing only |

If you are using a managed Flink environment, use that platform's documented connector delivery method rather than assuming local classpath behavior.

## Local PyFlink execution

Install the optional Python dependency first:

```bash
poetry install --with dev -E flink -E kafka
```

Run the job in file mode:

```bash
poetry run pyflink-fraud-job \
  --source file \
  --input data/sample_transactions.jsonl \
  --sink stdout
```

Run the job in Kafka mode:

```bash
poetry run pyflink-fraud-job \
  --source kafka \
  --bootstrap-servers localhost:9092 \
  --input-topic transactions \
  --output-topic fraud-alerts \
  --group-id fraud-detector
```

The Python command above is still not enough by itself. The Flink process must also be started with a Kafka connector JAR on the classpath or via the platform-specific connector directory.

## Docker execution

For Docker-based Flink demos, make the connector JAR assumption explicit:

- Bake the connector JAR into the image; or
- mount it into the container and reference the expected Flink lib/plugins directory.

Do not rely on an undeclared host-level Flink installation. Containerized demos should document the exact JAR origin and version.

## Runtime validation in this repository

The PyFlink wrapper now validates Kafka-related arguments before importing PyFlink:

- `--bootstrap-servers` is required for Kafka source or Kafka sink mode.
- `--input-topic` is required for Kafka source mode.
- `--output-topic` is required for Kafka sink mode.
- `--group-id` is required for Kafka source mode.
- `--input` is only valid for `--source=file`.

These checks prevent a class of avoidable runtime failures, but they do not replace connector JAR setup.

## Troubleshooting

Common failures and what they usually mean:

- `ModuleNotFoundError` or `PyFlink is not installed`
  Install the optional Python dependency with `poetry install --with dev -E flink`.

- Kafka connector classes cannot be found
  The Flink runtime is missing the Kafka connector JAR, or the JAR does not match the Flink runtime version.

- Kafka source/sink starts but cannot connect
  Check `--bootstrap-servers`, broker reachability, topic names, and local container networking.

- The job starts in Kafka mode but consumes nothing
  Verify the input topic has messages, confirm the consumer group offset behavior, and check whether you intended to read from the beginning.

- File mode works but Kafka mode fails immediately
  That usually points to connector configuration, not fraud logic. The business logic path is shared; the runtime plumbing is what changed.
