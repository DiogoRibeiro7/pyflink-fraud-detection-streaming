"""PyFlink DataStream job for real-time fraud detection."""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, Sequence

from fraud_streaming.features import compute_features, update_state
from fraud_streaming.rules import build_alert, score_features
from fraud_streaming.schemas import UserProfileState
from fraud_streaming.serialization import alert_to_json, transaction_from_json


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
    def build():
        """Build a KeyedProcessFunction subclass after PyFlink is available."""
        _require_pyflink()

        from pyflink.common.typeinfo import Types
        from pyflink.datastream import KeyedProcessFunction
        from pyflink.datastream.state import ValueStateDescriptor

        class _FraudProcessFunction(KeyedProcessFunction):
            def open(self, runtime_context):  # type: ignore[no-untyped-def]
                descriptor = ValueStateDescriptor("profile_state", Types.STRING())
                self.profile_state = runtime_context.get_state(descriptor)

            def process_element(self, value, ctx):  # type: ignore[no-untyped-def]
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


def _build_file_source(env, input_path: str):  # type: ignore[no-untyped-def]
    """Build a DataStream source from a local text file."""
    return env.read_text_file(input_path)


def _build_kafka_source(env, args: argparse.Namespace):  # type: ignore[no-untyped-def]
    """Build a Kafka source that emits JSON strings."""
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common.watermark_strategy import WatermarkStrategy
    from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

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


def _write_stdout(alert_stream) -> None:  # type: ignore[no-untyped-def]
    """Write alerts to stdout."""
    alert_stream.print()


def _write_kafka(alert_stream, args: argparse.Namespace) -> None:  # type: ignore[no-untyped-def]
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
    _require_pyflink()

    from pyflink.common.typeinfo import Types
    from pyflink.datastream import StreamExecutionEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)
    env.enable_checkpointing(30_000)

    if args.source == "file":
        if not args.input:
            raise ValueError("--input is required when --source=file")
        source_stream = _build_file_source(env, args.input)
    else:
        source_stream = _build_kafka_source(env, args)

    keyed = source_stream.key_by(lambda raw: transaction_from_json(raw).key)
    alert_stream = keyed.process(FraudProcessFunction.build(), output_type=Types.STRING())

    if args.sink == "kafka":
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
