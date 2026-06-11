# Flink Operations Guide

This guide is specific to the `pyflink-fraud-detection-streaming` repository. It explains the keyed state used by the current fraud pipeline, what checkpointing protects, where savepoints fit, and which operational settings matter most before treating the job as anything more than a local demo.

## What state this project keeps

The PyFlink job keys events by:

- `user_id`
- `card_id`

In code, the composite key is `"{user_id}:{card_id}"`.

For each key, the job stores a serialized `UserProfileState` value in Flink state. That state currently contains:

- transaction count statistics for amount mean and variance
- the last seen country
- the last seen device ID
- the last seen event timestamp in milliseconds
- a pruned list of recent rolling transactions used for velocity and one-hour amount features

The state value is stored as JSON through `UserProfileState.to_json()` and `UserProfileState.from_json()`.

## Why checkpointing matters here

Without checkpoints, any task restart can lose the fraud history that drives:

- amount z-score calculations
- recent transaction velocity
- one-hour amount aggregates
- country and device change signals
- minutes-since-last-transaction features

Because the scoring logic depends on prior keyed events, losing state does not just reduce throughput. It changes fraud scores and alert behaviour.

## Current runtime behaviour

The Flink wrapper currently exposes:

- `--parallelism`
- `--checkpoint-interval-ms`

Defaults:

- parallelism: `1`
- checkpoint interval: `30000` milliseconds

Disable checkpointing explicitly:

```bash
poetry run pyflink-fraud-job \
  --source file \
  --input data/sample_transactions.jsonl \
  --sink stdout \
  --checkpoint-interval-ms 0
```

Set a custom checkpoint interval:

```bash
poetry run pyflink-fraud-job \
  --source kafka \
  --bootstrap-servers localhost:9092 \
  --input-topic transactions \
  --output-topic fraud-alerts \
  --checkpoint-interval-ms 60000
```

Validation rules in the wrapper:

- `--checkpoint-interval-ms` must be non-negative
- `0` disables checkpointing
- positive values enable checkpointing at that interval

## Recommended local settings

For local demos:

- keep `--parallelism 1` unless you are explicitly testing partitioning behaviour
- keep checkpoints enabled if you want restart behaviour that roughly matches stateful production operation
- use `--checkpoint-interval-ms 30000` or `60000` for low-noise local runs

If you are just validating parsing or output shape on a bounded file input, disabling checkpoints can simplify the run:

```bash
poetry run pyflink-fraud-job \
  --source file \
  --input data/sample_transactions.jsonl \
  --sink stdout \
  --checkpoint-interval-ms 0
```

## Recommended production-style settings

For production-shaped environments, revisit at least:

- checkpoint interval
- checkpoint timeout
- minimum pause between checkpoints
- restart strategy
- state backend and checkpoint storage
- externalized checkpoint retention
- savepoint directory and artifact retention policy

The current repository exposes only the checkpoint interval because that is the safe part of the current wrapper to validate without adding a larger Flink config layer.

A reasonable production starting point often looks like:

- checkpoint interval: `30s` to `120s`
- durable checkpoint storage on object storage such as S3
- explicit restart strategy configured at cluster or job level
- stable savepoint location for controlled upgrades and incident rollback

The exact values depend on event volume, state size, acceptable replay cost, and downstream alerting tolerance.

## Savepoints versus checkpoints

Use checkpoints for:

- automatic fault recovery
- regular state snapshots used by Flink runtime recovery

Use savepoints for:

- controlled upgrades
- job migration
- rollback planning
- schema evolution testing

Operationally, think of savepoints as deliberate operator actions and checkpoints as routine recovery infrastructure.

## Restart strategies

This repository does not currently expose restart strategy flags in the Python CLI. That is intentional: restart behaviour is often easier to manage consistently at the cluster or deployment layer than as ad hoc per-run local flags.

For real deployments, choose an explicit restart strategy rather than relying on whatever cluster default happens to be present.

At minimum, decide:

- whether failures should restart automatically
- how many retries are acceptable
- how long to wait between retries
- what should happen when a connector dependency or schema issue causes a non-transient failure

## Event time and late events

The current Kafka/file PyFlink wrapper uses:

- `WatermarkStrategy.no_watermarks()`

That means the current wrapper is not using event-time watermarks for late-data handling. The pure fraud logic still reads `event_time` from each transaction, but Flink itself is not managing lateness semantics through watermark progression in this job today.

Operational implication:

- late or out-of-order events are still parsed and can affect keyed features
- there is no explicit watermark-driven late-event policy in the current wrapper
- if you later add event-time windows or timers, checkpoint and recovery behaviour must be re-evaluated with watermark semantics in mind

## State schema evolution risks

The most important upgrade risk in this project is `UserProfileState` compatibility.

Today the state is persisted as JSON with fields such as:

- `count`
- `amount_mean`
- `amount_m2`
- `last_country`
- `last_device_id`
- `last_event_time_ms`
- `rolling_transactions`

Risks during upgrades:

- renaming state fields without backward readers
- changing numeric meaning or units
- changing `rolling_transactions` element structure
- removing fields that older savepoints still contain
- changing the keying strategy from `user_id:card_id`

Safer evolution practices:

- add fields in backward-compatible ways with defaults
- keep old field names readable during migrations
- test `from_json()` against representative old state payloads before deployment
- take a savepoint before changing keyed state structure or key selection logic

## Suggested upgrade runbook

Before a stateful deployment change:

1. Record the current job version, connector version, and config.
2. Take a savepoint.
3. Validate the new code against representative historical state if the schema changed.
4. Deploy the new artifact with the same keying strategy unless a migration plan exists.
5. Restore from savepoint in a non-production environment first when possible.
6. Only then promote the upgrade to a higher environment.

## Troubleshooting checklist

If the job restarts and alerts suddenly look different:

- verify whether checkpointing was disabled
- confirm the job restored from expected state
- check whether the keying strategy changed
- inspect whether source offsets restarted from a different position
- verify that state JSON parsing still matches the stored payload structure

If recovery fails after a deployment:

- suspect state schema incompatibility first
- then check connector/runtime version mismatches
- then inspect savepoint or checkpoint storage permissions

## Related docs

- Connector setup: [`docs/flink-kafka-connectors.md`](flink-kafka-connectors.md)
- AWS deployment templates: [`infra/aws/README.md`](../infra/aws/README.md)
