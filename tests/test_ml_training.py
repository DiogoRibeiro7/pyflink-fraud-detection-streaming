from __future__ import annotations

import csv
import json
from pathlib import Path

from fraud_streaming.ml.training import (
    CANONICAL_FEATURE_SCHEMA,
    build_training_dataset,
    detect_input_format,
    evaluate_predictions,
    iter_training_payloads,
    validate_label_value,
)


def test_build_training_dataset_reuses_streaming_features() -> None:
    payloads = [
        {
            "transaction_id": "tx-1",
            "user_id": "user-1",
            "card_id": "card-1",
            "merchant_id": "merchant-1",
            "amount": 20.0,
            "currency": "EUR",
            "country": "PT",
            "device_id": "device-1",
            "merchant_category": "grocery",
            "event_time": "2026-06-10T12:00:00Z",
            "channel": "pos",
            "is_card_present": True,
        },
        {
            "transaction_id": "tx-2",
            "user_id": "user-1",
            "card_id": "card-1",
            "merchant_id": "merchant-2",
            "amount": 25.0,
            "currency": "EUR",
            "country": "US",
            "device_id": "device-2",
            "merchant_category": "travel",
            "event_time": "2026-06-10T12:01:00Z",
            "channel": "online",
            "is_card_present": False,
            "label": 1,
        },
    ]

    dataset = build_training_dataset(payloads)

    assert dataset.feature_schema == CANONICAL_FEATURE_SCHEMA
    assert len(dataset.examples) == 2
    assert dataset.examples[0].feature_values["tx_count_5m"] == 1.0
    assert dataset.examples[1].feature_values["tx_count_5m"] == 2.0
    assert dataset.examples[1].feature_values["country_changed"] == 1.0
    assert dataset.examples[1].label == 1
    assert dataset.examples[1].label_source == "input_label"


def test_validate_label_value_accepts_binary_inputs() -> None:
    assert validate_label_value(1) == 1
    assert validate_label_value(0) == 0
    assert validate_label_value(True) == 1
    assert validate_label_value("0") == 0


def test_validate_label_value_rejects_non_binary_values() -> None:
    try:
        validate_label_value(2)
    except ValueError as exc:
        assert "label must be binary" in str(exc)
    else:
        raise AssertionError("expected ValueError for non-binary label")


def test_evaluate_predictions_returns_threshold_analysis() -> None:
    metrics = evaluate_predictions([0, 1, 1, 0], [0.1, 0.8, 0.7, 0.2])

    assert metrics["precision"] >= 0.0
    assert metrics["recall"] >= 0.0
    assert metrics["f1"] >= 0.0
    assert metrics["roc_auc"] is not None
    assert len(metrics["threshold_table"]) == 4


def test_iter_training_payloads_reads_csv_input(tmp_path: Path) -> None:
    csv_path = tmp_path / "transactions.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "transaction_id",
                "user_id",
                "card_id",
                "merchant_id",
                "amount",
                "currency",
                "country",
                "device_id",
                "merchant_category",
                "event_time",
                "channel",
                "is_card_present",
                "fraud_flag",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "transaction_id": "tx-1",
                "user_id": "user-1",
                "card_id": "card-1",
                "merchant_id": "merchant-1",
                "amount": "25.0",
                "currency": "EUR",
                "country": "PT",
                "device_id": "device-1",
                "merchant_category": "grocery",
                "event_time": "2026-06-10T12:00:00Z",
                "channel": "pos",
                "is_card_present": "True",
                "fraud_flag": "1",
            }
        )

    payloads = list(
        iter_training_payloads(
            input_path=csv_path,
            input_format="csv",
            users=0,
            transactions=0,
            seed=0,
        )
    )
    dataset = build_training_dataset(payloads, label_column="fraud_flag", require_input_labels=True)

    assert detect_input_format(csv_path, "auto") == "csv"
    assert dataset.examples[0].label == 1
    assert dataset.examples[0].label_source == "input_label"


def test_build_training_dataset_requires_requested_label_column() -> None:
    payloads = [
        {
            "transaction_id": "tx-1",
            "user_id": "user-1",
            "card_id": "card-1",
            "merchant_id": "merchant-1",
            "amount": 20.0,
            "currency": "EUR",
            "country": "PT",
            "device_id": "device-1",
            "merchant_category": "grocery",
            "event_time": "2026-06-10T12:00:00Z",
            "channel": "pos",
            "is_card_present": True,
        }
    ]

    try:
        build_training_dataset(payloads, label_column="fraud_flag", require_input_labels=True)
    except ValueError as exc:
        assert "fraud_flag" in str(exc)
    else:
        raise AssertionError("expected missing label column validation to fail")


def test_iter_training_payloads_applies_dataset_mapping(tmp_path: Path) -> None:
    csv_path = tmp_path / "public_like.csv"
    mapping_path = tmp_path / "mapping.json"
    mapping_path.write_text(
        json.dumps(
            {
                "field_map": {
                    "transaction_id": "txn_id",
                    "user_id": "customer_id",
                    "card_id": "account_id",
                    "merchant_id": "merchant_code",
                    "amount": "amt",
                    "currency": "ccy",
                    "country": "country_code",
                    "device_id": "device_code",
                    "merchant_category": "mcc",
                    "event_time": "ts",
                    "channel": "entry_channel",
                    "is_card_present": "card_present",
                    "label": "fraud_flag",
                },
                "defaults": {"latitude": None, "longitude": None},
                "value_maps": {
                    "is_card_present": {"Y": True, "N": False},
                    "label": {"fraud": "1", "legit": "0"},
                },
            }
        ),
        encoding="utf-8",
    )
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "txn_id",
                "customer_id",
                "account_id",
                "merchant_code",
                "amt",
                "ccy",
                "country_code",
                "device_code",
                "mcc",
                "ts",
                "entry_channel",
                "card_present",
                "fraud_flag",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "txn_id": "tx-9",
                "customer_id": "user-9",
                "account_id": "card-9",
                "merchant_code": "merchant-9",
                "amt": "88.5",
                "ccy": "eur",
                "country_code": "pt",
                "device_code": "device-9",
                "mcc": "travel",
                "ts": "2026-06-10T12:00:00Z",
                "entry_channel": "online",
                "card_present": "N",
                "fraud_flag": "fraud",
            }
        )

    payloads = list(
        iter_training_payloads(
            input_path=csv_path,
            input_format="csv",
            dataset_mapping_path=mapping_path,
            users=0,
            transactions=0,
            seed=0,
        )
    )
    dataset = build_training_dataset(payloads, require_input_labels=True)

    assert payloads[0]["transaction_id"] == "tx-9"
    assert payloads[0]["is_card_present"] is False
    assert dataset.examples[0].label == 1
