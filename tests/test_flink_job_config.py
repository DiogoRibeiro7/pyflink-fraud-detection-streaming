from __future__ import annotations

import argparse

import pytest

from fraud_streaming.flink_job import is_late_event, validate_runtime_args


def make_args(**overrides: object) -> argparse.Namespace:
    payload: dict[str, object] = {
        "source": "file",
        "sink": "stdout",
        "input": "data/sample_transactions.jsonl",
        "bootstrap_servers": "localhost:9092",
        "input_topic": "transactions",
        "output_topic": "fraud-alerts",
        "group_id": "fraud-detector",
        "parallelism": 1,
        "checkpoint_interval_ms": 30_000,
        "watermark_max_out_of_orderness_ms": None,
        "allowed_lateness_ms": 0,
        "late_event_policy": "process",
        "scoring_strategy": "rules",
        "model_artifact": None,
        "rule_weight": 0.5,
        "model_weight": 0.5,
    }
    payload.update(overrides)
    return argparse.Namespace(**payload)


def test_validate_runtime_args_accepts_file_to_stdout_configuration() -> None:
    config = validate_runtime_args(make_args())

    assert config.source == "file"
    assert config.sink == "stdout"
    assert config.input_path == "data/sample_transactions.jsonl"
    assert config.checkpoint_interval_ms == 30_000


def test_validate_runtime_args_requires_input_for_file_source() -> None:
    with pytest.raises(ValueError, match="--input is required when --source=file"):
        validate_runtime_args(make_args(input=""))


def test_validate_runtime_args_requires_bootstrap_servers_for_kafka_source() -> None:
    with pytest.raises(ValueError, match="--bootstrap-servers is required when --source=kafka"):
        validate_runtime_args(make_args(source="kafka", input=None, bootstrap_servers=""))


def test_validate_runtime_args_requires_input_topic_for_kafka_source() -> None:
    with pytest.raises(ValueError, match="--input-topic is required when --source=kafka"):
        validate_runtime_args(make_args(source="kafka", input=None, input_topic=""))


def test_validate_runtime_args_rejects_file_input_for_kafka_source() -> None:
    with pytest.raises(ValueError, match="--input is only supported when --source=file"):
        validate_runtime_args(make_args(source="kafka", input="data/sample_transactions.jsonl"))


def test_validate_runtime_args_requires_output_topic_for_kafka_sink() -> None:
    with pytest.raises(ValueError, match="--output-topic is required when --sink=kafka"):
        validate_runtime_args(make_args(sink="kafka", output_topic=""))


def test_validate_runtime_args_rejects_non_positive_parallelism() -> None:
    with pytest.raises(ValueError, match="--parallelism must be positive"):
        validate_runtime_args(make_args(parallelism=0))


def test_validate_runtime_args_rejects_negative_checkpoint_interval() -> None:
    with pytest.raises(ValueError, match="--checkpoint-interval-ms must be non-negative"):
        validate_runtime_args(make_args(checkpoint_interval_ms=-1))


def test_validate_runtime_args_allows_checkpointing_to_be_disabled() -> None:
    config = validate_runtime_args(make_args(checkpoint_interval_ms=0))

    assert config.checkpoint_interval_ms is None


def test_validate_runtime_args_rejects_negative_watermark_delay() -> None:
    with pytest.raises(
        ValueError,
        match="--watermark-max-out-of-orderness-ms must be non-negative",
    ):
        validate_runtime_args(make_args(watermark_max_out_of_orderness_ms=-1))


def test_validate_runtime_args_requires_watermark_for_drop_policy() -> None:
    with pytest.raises(
        ValueError,
        match="--late-event-policy=drop requires --watermark-max-out-of-orderness-ms to be set",
    ):
        validate_runtime_args(make_args(late_event_policy="drop"))


def test_is_late_event_respects_current_watermark_and_allowed_lateness() -> None:
    assert (
        is_late_event(event_time_ms=1000, current_watermark_ms=-1, allowed_lateness_ms=0) is False
    )
    assert (
        is_late_event(event_time_ms=1000, current_watermark_ms=1500, allowed_lateness_ms=250)
        is True
    )
    assert (
        is_late_event(event_time_ms=1000, current_watermark_ms=1200, allowed_lateness_ms=250)
        is False
    )


def test_validate_runtime_args_requires_model_artifact_for_model_strategy() -> None:
    with pytest.raises(ValueError, match="model_artifact_path is required"):
        validate_runtime_args(make_args(scoring_strategy="model"))
