"""PyFlink DataStream job for real-time fraud detection."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from fraud_streaming.features import compute_features, update_state
from fraud_streaming.ml.scoring import (
    ModelScorer,
    ScoringConfig,
    combine_scores,
    compute_model_score,
)
from fraud_streaming.rules import build_alert, score_features
from fraud_streaming.schemas import UserProfileState
from fraud_streaming.serialization import alert_to_json, transaction_from_json


@dataclass(frozen=True, slots=True)
class FlinkJobConfig:
    """Validated runtime configuration for the PyFlink job wrapper."""

    source: str
    sink: str
    input_path: str | None
    bootstrap_servers: str
    input_topic: str
    output_topic: str
    group_id: str
    parallelism: int
    scoring_strategy: str
    model_artifact_path: Path | None
    rule_weight: float
    model_weight: float
    checkpoint_interval_ms: int | None
    watermark_max_out_of_orderness_ms: int | None
    allowed_lateness_ms: int
    late_event_policy: Literal["process", "drop"]


def _require_pyflink() -> None:
    """Raise a helpful error when PyFlink is not installed."""
    try:
        import pyflink  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "PyFlink is not installed. Install it with `poetry install -E flink` "
            "or `pip install apache-flink`."
        ) from exc


class FraudProcessFunction:  # pragma: no cover - exercised only with PyFlink runtime
    """Keyed PyFlink process function that scores transactions.

    The class is created dynamically to avoid importing PyFlink during unit tests.
    """

    @staticmethod
    def build(
        scoring_config: ScoringConfig,
        model_scorer: ModelScorer | None,
        *,
        allowed_lateness_ms: int,
        late_event_policy: Literal["process", "drop"],
    ) -> Any:
        """Build a KeyedProcessFunction subclass after PyFlink is available."""
        _require_pyflink()

        from pyflink.common.typeinfo import Types
        from pyflink.datastream import KeyedProcessFunction
        from pyflink.datastream.state import ValueStateDescriptor

        class _FraudProcessFunction(KeyedProcessFunction):  # type: ignore[misc]
            def open(self, runtime_context: Any) -> None:
                descriptor = ValueStateDescriptor("profile_state", Types.STRING())
                self.profile_state = runtime_context.get_state(descriptor)

            def process_element(self, value: str, ctx: Any) -> Any:
                transaction = transaction_from_json(value)
                current_watermark = ctx.timer_service().current_watermark()
                if late_event_policy == "drop" and is_late_event(
                    event_time_ms=transaction.event_time_ms,
                    current_watermark_ms=current_watermark,
                    allowed_lateness_ms=allowed_lateness_ms,
                ):
                    return
                state = UserProfileState.from_json(self.profile_state.value())
                features = compute_features(transaction, state)
                rule_score = score_features(features)
                model_score = None
                if scoring_config.strategy in {"model", "blend"}:
                    if model_scorer is None:
                        raise ValueError("model_scorer is required for model or blend strategy")
                    model_score = compute_model_score(model_scorer, features, transaction)
                final_score = combine_scores(
                    rule_score=rule_score,
                    model_score=model_score,
                    strategy=scoring_config.strategy,
                    rule_weight=scoring_config.rule_weight,
                    model_weight=scoring_config.model_weight,
                )
                alert = build_alert(features, final_score)
                updated_state = update_state(transaction, state)
                self.profile_state.update(updated_state.to_json())

                if alert.risk_level != "low":
                    yield alert_to_json(alert)

        return _FraudProcessFunction()


def build_parser() -> argparse.ArgumentParser:
    """Create the PyFlink job argument parser."""
    parser = argparse.ArgumentParser(description="Run the PyFlink fraud detection job.")
    parser.add_argument("--source", choices=["file", "kafka"], required=True)
    parser.add_argument("--sink", choices=["stdout", "kafka"], default="stdout")
    parser.add_argument("--input", help="Input JSONL file path when --source=file.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--input-topic", default="transactions")
    parser.add_argument("--output-topic", default="fraud-alerts")
    parser.add_argument("--group-id", default="fraud-detector")
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument(
        "--checkpoint-interval-ms",
        type=int,
        default=30_000,
        help="Checkpoint interval in milliseconds. Use 0 to disable checkpointing.",
    )
    parser.add_argument(
        "--scoring-strategy",
        choices=["rules", "model", "blend"],
        default="rules",
    )
    parser.add_argument(
        "--model-artifact",
        help="Optional model artifact path for model-aware scoring.",
    )
    parser.add_argument("--rule-weight", type=float, default=0.5)
    parser.add_argument("--model-weight", type=float, default=0.5)
    parser.add_argument(
        "--watermark-max-out-of-orderness-ms",
        type=int,
        help="Enable event-time watermarks with bounded out-of-orderness in milliseconds.",
    )
    parser.add_argument(
        "--allowed-lateness-ms",
        type=int,
        default=0,
        help="Allowed lateness in milliseconds before a record is treated as late.",
    )
    parser.add_argument(
        "--late-event-policy",
        choices=["process", "drop"],
        default="process",
        help="How to handle late events once watermarks are enabled.",
    )
    return parser


def is_late_event(
    *, event_time_ms: int, current_watermark_ms: int, allowed_lateness_ms: int
) -> bool:
    """Return whether an event falls behind the current watermark beyond tolerance."""
    if current_watermark_ms < 0:
        return False
    return event_time_ms + allowed_lateness_ms < current_watermark_ms


def validate_runtime_args(args: argparse.Namespace) -> FlinkJobConfig:
    """Validate source and sink configuration before importing PyFlink."""
    bootstrap_servers = args.bootstrap_servers.strip()
    input_topic = args.input_topic.strip()
    output_topic = args.output_topic.strip()
    group_id = args.group_id.strip()
    input_path = args.input.strip() if isinstance(args.input, str) else args.input
    checkpoint_interval_ms = args.checkpoint_interval_ms
    watermark_max_out_of_orderness_ms = args.watermark_max_out_of_orderness_ms
    allowed_lateness_ms = args.allowed_lateness_ms

    if args.parallelism <= 0:
        raise ValueError("--parallelism must be positive")
    if checkpoint_interval_ms < 0:
        raise ValueError("--checkpoint-interval-ms must be non-negative")
    if watermark_max_out_of_orderness_ms is not None and watermark_max_out_of_orderness_ms < 0:
        raise ValueError("--watermark-max-out-of-orderness-ms must be non-negative")
    if allowed_lateness_ms < 0:
        raise ValueError("--allowed-lateness-ms must be non-negative")
    if args.late_event_policy == "drop" and watermark_max_out_of_orderness_ms is None:
        raise ValueError(
            "--late-event-policy=drop requires --watermark-max-out-of-orderness-ms to be set"
        )

    if args.source == "file":
        if not input_path:
            raise ValueError("--input is required when --source=file")
    elif args.source == "kafka":
        if input_path:
            raise ValueError("--input is only supported when --source=file")
        if not bootstrap_servers:
            raise ValueError("--bootstrap-servers is required when --source=kafka")
        if not input_topic:
            raise ValueError("--input-topic is required when --source=kafka")
        if not group_id:
            raise ValueError("--group-id is required when --source=kafka")
    else:
        raise ValueError(f"unsupported source: {args.source}")

    if args.sink == "kafka":
        if not bootstrap_servers:
            raise ValueError("--bootstrap-servers is required when --sink=kafka")
        if not output_topic:
            raise ValueError("--output-topic is required when --sink=kafka")
    elif args.sink != "stdout":
        raise ValueError(f"unsupported sink: {args.sink}")

    scoring_config = ScoringConfig(
        strategy=args.scoring_strategy,
        model_artifact_path=(
            Path(args.model_artifact) if args.model_artifact is not None else None
        ),
        rule_weight=args.rule_weight,
        model_weight=args.model_weight,
    )

    return FlinkJobConfig(
        source=args.source,
        sink=args.sink,
        input_path=input_path,
        bootstrap_servers=bootstrap_servers,
        input_topic=input_topic,
        output_topic=output_topic,
        group_id=group_id,
        parallelism=args.parallelism,
        scoring_strategy=scoring_config.strategy,
        model_artifact_path=scoring_config.model_artifact_path,
        rule_weight=scoring_config.rule_weight,
        model_weight=scoring_config.model_weight,
        checkpoint_interval_ms=(None if checkpoint_interval_ms == 0 else checkpoint_interval_ms),
        watermark_max_out_of_orderness_ms=watermark_max_out_of_orderness_ms,
        allowed_lateness_ms=allowed_lateness_ms,
        late_event_policy=args.late_event_policy,
    )


def _build_watermark_strategy(max_out_of_orderness_ms: int | None) -> Any:
    """Build a PyFlink watermark strategy for transaction JSON strings."""
    from pyflink.common.time import Duration
    from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy

    if max_out_of_orderness_ms is None:
        return WatermarkStrategy.no_watermarks()

    class _TransactionTimestampAssigner(TimestampAssigner):  # type: ignore[misc]
        def extract_timestamp(self, value: str, record_timestamp: int) -> int:
            del record_timestamp
            return transaction_from_json(value).event_time_ms

    return WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_millis(max_out_of_orderness_ms)
    ).with_timestamp_assigner(_TransactionTimestampAssigner())


def _build_file_source(env: Any, input_path: str, watermark_strategy: Any) -> Any:
    """Build a DataStream source from a local text file."""
    source_stream = env.read_text_file(input_path)
    return source_stream.assign_timestamps_and_watermarks(watermark_strategy)


def _build_kafka_source(env: Any, args: argparse.Namespace, watermark_strategy: Any) -> Any:
    """Build a Kafka source that emits JSON strings."""
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.bootstrap_servers)
        .set_topics(args.input_topic)
        .set_group_id(args.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return env.from_source(source, watermark_strategy, "transactions-kafka-source")


def _write_stdout(alert_stream: Any) -> None:
    """Write alerts to stdout."""
    alert_stream.print()


def _write_kafka(alert_stream: Any, args: argparse.Namespace) -> None:
    """Write alerts to Kafka."""
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.connector.kafka import KafkaRecordSerializationSchema, KafkaSink

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(args.bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(args.output_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    alert_stream.sink_to(sink)


def run_job(args: argparse.Namespace) -> None:  # pragma: no cover - requires PyFlink runtime
    """Run the configured PyFlink fraud detection job."""
    config = validate_runtime_args(args)
    scoring_config = ScoringConfig(
        strategy=config.scoring_strategy,
        model_artifact_path=config.model_artifact_path,
        rule_weight=config.rule_weight,
        model_weight=config.model_weight,
    )
    model_scorer = (
        None
        if scoring_config.strategy == "rules"
        else ModelScorer.from_artifact(
            config.model_artifact_path if config.model_artifact_path is not None else Path("")
        )
    )
    _require_pyflink()

    from pyflink.common.typeinfo import Types
    from pyflink.datastream import StreamExecutionEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config.parallelism)
    if config.checkpoint_interval_ms is not None:
        env.enable_checkpointing(config.checkpoint_interval_ms)
    watermark_strategy = _build_watermark_strategy(config.watermark_max_out_of_orderness_ms)

    if config.source == "file":
        if config.input_path is None:
            raise ValueError("--input is required when --source=file")
        source_stream = _build_file_source(env, config.input_path, watermark_strategy)
    else:
        source_stream = _build_kafka_source(env, args, watermark_strategy)

    keyed = source_stream.key_by(lambda raw: transaction_from_json(raw).key)
    alert_stream = keyed.process(
        FraudProcessFunction.build(
            scoring_config,
            model_scorer,
            allowed_lateness_ms=config.allowed_lateness_ms,
            late_event_policy=config.late_event_policy,
        ),
        output_type=Types.STRING(),
    )

    if config.sink == "kafka":
        _write_kafka(alert_stream, args)
    else:
        _write_stdout(alert_stream)

    env.execute("pyflink-fraud-detection-streaming")


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for the PyFlink job."""
    parser = build_parser()
    args = parser.parse_args(argv)
    run_job(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
