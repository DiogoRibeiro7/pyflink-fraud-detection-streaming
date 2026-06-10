from __future__ import annotations

from datetime import UTC, datetime

from fraud_streaming.rules import score_features
from fraud_streaming.schemas import FraudFeatures


def test_score_features_returns_high_risk_for_combined_signals() -> None:
    features = FraudFeatures(
        transaction_id="tx-1",
        user_id="user-1",
        card_id="card-1",
        event_time=datetime(2026, 6, 10, 23, 0, tzinfo=UTC),
        amount=1_200.0,
        tx_count_5m=6,
        amount_sum_1h=3_000.0,
        amount_zscore=4.5,
        minutes_since_last_tx=5.0,
        country_changed=True,
        device_changed=True,
        card_not_present=True,
        night_transaction=True,
        high_velocity=True,
        high_amount=True,
    )

    result = score_features(features)

    assert result.risk_level == "high"
    assert result.risk_score == 100
    assert "transaction velocity is high" in result.reasons
    assert "amount is unusual for the user" in result.reasons


def test_score_features_returns_low_risk_when_no_rules_fire() -> None:
    features = FraudFeatures(
        transaction_id="tx-1",
        user_id="user-1",
        card_id="card-1",
        event_time=datetime(2026, 6, 10, 12, 0, tzinfo=UTC),
        amount=15.0,
        tx_count_5m=1,
        amount_sum_1h=15.0,
        amount_zscore=0.2,
        minutes_since_last_tx=None,
        country_changed=False,
        device_changed=False,
        card_not_present=False,
        night_transaction=False,
        high_velocity=False,
        high_amount=False,
    )

    result = score_features(features)

    assert result.risk_level == "low"
    assert result.risk_score == 0
    assert result.reasons == []
