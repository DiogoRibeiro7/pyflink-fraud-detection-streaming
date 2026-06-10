from __future__ import annotations

import io
import json
from datetime import datetime, timezone

from fraud_streaming.observability.metrics import LocalMetricsRegistry
from fraud_streaming.schemas import Alert, FraudFeatures, Transaction


def make_transaction(country: str = "PT", channel: str = "pos") -> Transaction:
    return Transaction(
        transaction_id="tx-1",
        user_id="user-1",
        card_id="card-1",
        merchant_id="merchant-1",
        amount=42.0,
        currency="EUR",
        country=country,
        device_id="device-1",
        merchant_category="grocery",
        event_time=datetime(2026, 6, 10, 12, 0, tzinfo=timezone.utc),
        channel=channel,
        is_card_present=channel != "online",
    )


def make_alert(risk_level: str = "medium", risk_score: int = 55) -> Alert:
    features = FraudFeatures(
        transaction_id="tx-1",
        user_id="user-1",
        card_id="card-1",
        event_time=datetime(2026, 6, 10, 12, 0, tzinfo=timezone.utc),
        amount=42.0,
        tx_count_5m=3,
        amount_sum_1h=100.0,
        amount_zscore=0.5,
        minutes_since_last_tx=1.0,
        country_changed=False,
        device_changed=False,
        card_not_present=False,
        night_transaction=False,
        high_velocity=False,
        high_amount=False,
    )
    return Alert(
        transaction_id="tx-1",
        user_id="user-1",
        card_id="card-1",
        event_time=features.event_time,
        risk_score=risk_score,
        risk_level=risk_level,  # type: ignore[arg-type]
        reasons=[],
        features=features,
    )


def test_local_metrics_registry_tracks_counters_and_gauges() -> None:
    registry = LocalMetricsRegistry()

    registry.record_processed_transaction(
        make_transaction(country="PT", channel="pos"),
        make_alert(),
    )
    registry.record_emitted_alert(make_alert(risk_level="high", risk_score=80))
    registry.record_malformed_event()

    text = registry.to_prometheus_text()

    assert "fraud_transactions_processed_total 1" in text
    assert 'fraud_events_by_country_total{country="PT"} 1' in text
    assert 'fraud_events_by_channel_total{channel="pos"} 1' in text
    assert 'fraud_events_by_risk_level_total{risk_level="medium"} 1' in text
    assert "fraud_alerts_emitted_total 1" in text
    assert "fraud_high_risk_alerts_total 1" in text
    assert "fraud_malformed_events_total 1" in text
    assert "fraud_average_risk_score 55" in text


def test_process_json_lines_updates_metrics_for_valid_and_invalid_events() -> None:
    from fraud_streaming.local_runner import process_json_lines

    lines = [
        json.dumps(
            {
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
                "is_card_present": True,
            }
        ),
        "{bad json",
    ]
    registry = LocalMetricsRegistry()
    dead_letters = io.StringIO()

    list(
        process_json_lines(
            lines,
            emit_low_risk=True,
            dead_letter_handle=dead_letters,
            metrics=registry,
        )
    )

    text = registry.to_prometheus_text()
    assert "fraud_transactions_processed_total 1" in text
    assert "fraud_malformed_events_total 1" in text
