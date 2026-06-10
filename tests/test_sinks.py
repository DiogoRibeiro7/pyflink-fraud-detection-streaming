from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from fraud_streaming.features import compute_features
from fraud_streaming.local_runner import process_json_lines
from fraud_streaming.rules import build_alert, score_features
from fraud_streaming.schemas import Alert, Transaction, UserProfileState
from fraud_streaming.sinks.local import (
    JsonlAlertSink,
    JsonlTransactionSink,
    validate_local_sink_args,
)


def make_transaction(transaction_id: str = "tx-1", amount: float = 42.0) -> Transaction:
    return Transaction(
        transaction_id=transaction_id,
        user_id="user-1",
        card_id="card-1",
        merchant_id="merchant-1",
        amount=amount,
        currency="EUR",
        country="PT",
        device_id="device-1",
        merchant_category="grocery",
        event_time=datetime(2026, 6, 10, 12, 0, tzinfo=timezone.utc),
        channel="pos",
        is_card_present=True,
    )


def make_alert() -> Alert:
    transaction = make_transaction()
    features = compute_features(transaction, UserProfileState())
    return build_alert(features, score_features(features))


def make_args(**overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "alert_sink": "stdout",
        "alert_output": None,
        "transaction_sink": "none",
        "transaction_output": None,
        "iceberg_catalog_uri": None,
        "iceberg_warehouse": None,
        "iceberg_alert_table": None,
        "iceberg_transaction_table": None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_validate_local_sink_args_accepts_default_stdout_configuration() -> None:
    config = validate_local_sink_args(make_args())

    assert config.alert_sink == "stdout"
    assert config.transaction_sink == "none"


def test_validate_local_sink_args_requires_output_paths_for_file_sinks() -> None:
    with pytest.raises(ValueError, match="--alert-output is required"):
        validate_local_sink_args(make_args(alert_sink="jsonl"))

    with pytest.raises(ValueError, match="--transaction-output is required"):
        validate_local_sink_args(make_args(transaction_sink="jsonl"))


def test_validate_local_sink_args_requires_iceberg_configuration() -> None:
    with pytest.raises(ValueError, match="--iceberg-catalog-uri is required"):
        validate_local_sink_args(make_args(alert_sink="iceberg", alert_output=Path("ignored")))


def test_jsonl_alert_sink_writes_canonical_alert_json(tmp_path: Path) -> None:
    output = tmp_path / "alerts.jsonl"
    sink = JsonlAlertSink(output)

    sink.write(make_alert())
    sink.close()

    lines = output.read_text(encoding="utf-8").strip().splitlines()
    payload = json.loads(lines[0])
    assert payload["transaction_id"] == "tx-1"
    assert payload["features"]["amount"] == 42.0


def test_jsonl_transaction_sink_writes_canonical_transaction_json(tmp_path: Path) -> None:
    output = tmp_path / "transactions.jsonl"
    sink = JsonlTransactionSink(output)

    sink.write(make_transaction())
    sink.close()

    lines = output.read_text(encoding="utf-8").strip().splitlines()
    payload = json.loads(lines[0])
    assert payload["transaction_id"] == "tx-1"
    assert payload["amount"] == 42.0


def test_process_json_lines_can_write_transaction_and_alert_sinks(tmp_path: Path) -> None:
    alert_output = tmp_path / "alerts.jsonl"
    transaction_output = tmp_path / "transactions.jsonl"
    alert_sink = JsonlAlertSink(alert_output)
    transaction_sink = JsonlTransactionSink(transaction_output)
    line = json.dumps(make_transaction().to_dict())

    alerts = list(
        process_json_lines(
            [line],
            emit_low_risk=True,
            transaction_sink=transaction_sink,
            alert_sink=alert_sink,
        )
    )
    alert_sink.close()
    transaction_sink.close()

    assert len(alerts) == 1
    assert '"transaction_id":"tx-1"' in alert_output.read_text(encoding="utf-8")
    assert '"transaction_id":"tx-1"' in transaction_output.read_text(encoding="utf-8")
