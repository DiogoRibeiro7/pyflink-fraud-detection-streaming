from __future__ import annotations

from fraud_streaming.ml.training import (
    CANONICAL_FEATURE_SCHEMA,
    build_training_dataset,
    evaluate_predictions,
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
