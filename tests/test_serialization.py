from __future__ import annotations

import json

import pytest

from fraud_streaming.serialization import alert_from_json, transaction_from_json


def test_transaction_from_json_parses_valid_payload() -> None:
    payload = {
        "transaction_id": "tx-1",
        "user_id": "user-1",
        "card_id": "card-1",
        "merchant_id": "merchant-1",
        "amount": 42.0,
        "currency": "eur",
        "country": "pt",
        "device_id": "device-1",
        "merchant_category": "grocery",
        "event_time": "2026-06-10T12:00:00Z",
        "channel": "pos",
        "is_card_present": True,
    }

    transaction = transaction_from_json(json.dumps(payload))

    assert transaction.transaction_id == "tx-1"
    assert transaction.currency == "EUR"
    assert transaction.country == "PT"
    assert transaction.event_time.tzinfo is not None


def test_transaction_from_json_rejects_missing_required_field() -> None:
    payload = {
        "transaction_id": "tx-1",
        "user_id": "user-1",
        "card_id": "card-1",
        "merchant_id": "merchant-1",
        "amount": 42.0,
        "currency": "EUR",
        "country": "PT",
        "device_id": "device-1",
        "merchant_category": "grocery",
        "event_time": "2026-06-10T12:00:00Z",
        "channel": "pos",
    }

    with pytest.raises(ValueError, match="is_card_present"):
        transaction_from_json(json.dumps(payload))


def test_alert_from_json_parses_valid_payload() -> None:
    payload = {
        "transaction_id": "tx-1",
        "user_id": "user-1",
        "card_id": "card-1",
        "event_time": "2026-06-10T12:00:00Z",
        "risk_score": 80,
        "risk_level": "high",
        "reasons": ["transaction velocity is high"],
        "features": {
            "amount": 42.0,
            "tx_count_5m": 5,
            "amount_sum_1h": 120.0,
            "amount_zscore": 2.5,
            "minutes_since_last_tx": 1.0,
            "country_changed": False,
            "device_changed": False,
            "card_not_present": False,
            "night_transaction": False,
            "high_velocity": True,
            "high_amount": False,
        },
    }

    alert = alert_from_json(json.dumps(payload))

    assert alert.transaction_id == "tx-1"
    assert alert.risk_level == "high"
    assert alert.features.high_velocity is True
