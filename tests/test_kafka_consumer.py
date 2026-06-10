from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

import pytest

from fraud_streaming.kafka.consumer import (
    alert_matches_filters,
    parse_alert_message,
    summarize_alerts,
    validate_args,
)
from fraud_streaming.schemas import Alert, FraudFeatures
from fraud_streaming.serialization import alert_to_json


def make_alert(risk_level: str = "high", risk_score: int = 85) -> Alert:
    features = FraudFeatures(
        transaction_id="tx-1",
        user_id="user-1",
        card_id="card-1",
        event_time=datetime(2026, 6, 10, 12, 0, tzinfo=timezone.utc),
        amount=120.0,
        tx_count_5m=5,
        amount_sum_1h=400.0,
        amount_zscore=2.0,
        minutes_since_last_tx=1.0,
        country_changed=False,
        device_changed=False,
        card_not_present=False,
        night_transaction=False,
        high_velocity=True,
        high_amount=False,
    )
    return Alert(
        transaction_id="tx-1",
        user_id="user-1",
        card_id="card-1",
        event_time=features.event_time,
        risk_score=risk_score,
        risk_level=risk_level,  # type: ignore[arg-type]
        reasons=["transaction velocity is high"],
        features=features,
    )


def test_parse_alert_message_round_trips_canonical_alert_json() -> None:
    alert = make_alert()

    parsed = parse_alert_message(alert_to_json(alert).encode("utf-8"))

    assert parsed.transaction_id == "tx-1"
    assert parsed.risk_level == "high"
    assert parsed.features.tx_count_5m == 5


def test_parse_alert_message_rejects_invalid_payload() -> None:
    payload = json.dumps({"risk_level": "high"})

    with pytest.raises(ValueError, match="features must be an object"):
        parse_alert_message(payload)


def test_alert_matches_filters_applies_risk_level_and_score() -> None:
    alert = make_alert(risk_level="medium", risk_score=55)

    assert alert_matches_filters(alert, risk_level="medium", min_risk_score=50) is True
    assert alert_matches_filters(alert, risk_level="high", min_risk_score=50) is False
    assert alert_matches_filters(alert, risk_level="medium", min_risk_score=60) is False


def test_summarize_alerts_counts_each_risk_level() -> None:
    alerts = [
        make_alert(risk_level="high", risk_score=90),
        make_alert(risk_level="medium", risk_score=55),
        make_alert(risk_level="medium", risk_score=45),
    ]

    summary = summarize_alerts(alerts)

    assert summary.total == 3
    assert summary.by_risk_level["high"] == 1
    assert summary.by_risk_level["medium"] == 2
    assert summary.by_risk_level["low"] == 0


def test_validate_args_rejects_invalid_min_risk_score() -> None:
    args = argparse.Namespace(
        bootstrap_servers="localhost:9092",
        topic="fraud-alerts",
        group_id="group-1",
        max_messages=10,
        min_risk_score=101,
    )

    with pytest.raises(ValueError, match="--min-risk-score must be between 0 and 100"):
        validate_args(args)
