"""PyFlink DataStream job for real-time fraud detection."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from fraud_streaming.features import compute_features, update_state
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
    def build() -> Any:
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
                state = UserProfileState.from_json(self.profile_state.value())
                features = compute_features(transaction, state)
                score = score_features(features)
                alert = build_alert(features, score)
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
    return parser


def validate_runtime_args(args: argparse.Namespace) -> FlinkJobConfig:
    """Validate source and sink configuration before importing PyFlink."""
    bootstrap_servers = args.bootstrap_servers.strip()
    input_topic = args.input_topic.strip()
    output_topic = args.output_topic.strip()
    group_id = args.group_id.strip()
    input_path = args.input.strip() if isinstance(args.input, str) else args.input

    if args.parallelism <= 0:
        raise ValueError("--parallelism must be positive")

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

    return FlinkJobConfig(
        source=args.source,
        sink=args.sink,
        input_path=input_path,
        bootstrap_servers=bootstrap_servers,
        input_topic=input_topic,
        output_topic=output_topic,
        group_id=group_id,
        parallelism=args.parallelism,
    )


def _build_file_source(env: Any, input_path: str) -> Any:
    """Build a DataStream source from a local text file."""
    return env.read_text_file(input_path)


def _build_kafka_source(env: Any, args: argparse.Namespace) -> Any:
    """Build a Kafka source that emits JSON strings."""
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
    from pyflink.common.watermark_strategy import WatermarkStrategy

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.bootstrap_servers)
        .set_topics(args.input_topic)
        .set_group_id(args.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return env.from_source(source, WatermarkStrategy.no_watermarks(), "transactions-kafka-source")


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
    _require_pyflink()

    from pyflink.common.typeinfo import Types
    from pyflink.datastream import StreamExecutionEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config.parallelism)
    env.enable_checkpointing(30_000)

    if config.source == "file":
        if config.input_path is None:
            raise ValueError("--input is required when --source=file")
        source_stream = _build_file_source(env, config.input_path)
    else:
        source_stream = _build_kafka_source(env, args)

    keyed = source_stream.key_by(lambda raw: transaction_from_json(raw).key)
    alert_stream = keyed.process(FraudProcessFunction.build(), output_type=Types.STRING())

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
