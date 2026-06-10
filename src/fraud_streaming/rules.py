"""Explainable fraud scoring rules."""

from __future__ import annotations

from dataclasses import dataclass

from fraud_streaming.config import DEFAULT_CONFIG, FraudConfig
from fraud_streaming.schemas import Alert, FraudFeatures, RiskLevel


@dataclass(frozen=True, slots=True)
class RuleScore:
    """Output of the transparent rule scorer."""

    risk_score: int
    risk_level: RiskLevel
    reasons: list[str]


def risk_level_from_score(score: int) -> RiskLevel:
    """Map a numeric score to a risk level."""
    if score >= 70:
        return "high"
    if score >= 40:
        return "medium"
    if score >= 20:
        return "elevated"
    return "low"


def score_features(
    features: FraudFeatures,
    config: FraudConfig = DEFAULT_CONFIG,
) -> RuleScore:
    """Score fraud risk using transparent, analyst-readable rules.

    Args:
        features: Features computed for a single transaction.
        config: Fraud thresholds.

    Returns:
        RuleScore with bounded numeric score and explanations.
    """
    if not isinstance(features, FraudFeatures):
        raise TypeError("features must be a FraudFeatures instance")

    score = 0
    reasons: list[str] = []

    if features.high_velocity:
        score += 25
        reasons.append("transaction velocity is high")

    if features.amount_sum_1h >= config.high_hourly_amount_threshold:
        score += 20
        reasons.append("one-hour transaction amount is high")

    if features.amount_zscore >= config.zscore_threshold:
        score += 25
        reasons.append("amount is unusual for the user")

    if features.country_changed:
        score += 15
        reasons.append("country changed recently")
        if features.minutes_since_last_tx is not None and features.minutes_since_last_tx <= 60:
            score += 10
            reasons.append("country changed within one hour of the previous transaction")

    if features.device_changed:
        score += 15
        reasons.append("device changed recently")

    if features.card_not_present and features.high_amount:
        score += 10
        reasons.append("high-value card-not-present transaction")

    if features.night_transaction and features.high_amount:
        score += 10
        reasons.append("high-value transaction occurred at night")

    bounded_score = min(score, 100)
    return RuleScore(
        risk_score=bounded_score,
        risk_level=risk_level_from_score(bounded_score),
        reasons=reasons,
    )


def build_alert(features: FraudFeatures, score: RuleScore) -> Alert:
    """Build an alert object from features and score output."""
    return Alert(
        transaction_id=features.transaction_id,
        user_id=features.user_id,
        card_id=features.card_id,
        event_time=features.event_time,
        risk_score=score.risk_score,
        risk_level=score.risk_level,
        reasons=score.reasons,
        features=features,
    )
