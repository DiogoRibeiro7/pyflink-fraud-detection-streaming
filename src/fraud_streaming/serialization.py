"""JSON serialization helpers for stream records."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, cast

from fraud_streaming.schemas import Alert, FraudFeatures, RiskLevel, Transaction


def parse_event_time(value: str) -> datetime:
    """Parse an ISO-8601 timestamp into a timezone-aware datetime."""
    if not isinstance(value, str):
        raise TypeError("event_time must be a string")
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError("event_time must include timezone information")
    return parsed.astimezone(timezone.utc)


def _required_str(payload: dict[str, Any], field: str) -> str:
    """Read a required string field from a decoded JSON object."""
    value = payload.get(field)
    if not isinstance(value, str) or value == "":
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _optional_float(payload: dict[str, Any], field: str) -> float | None:
    """Read an optional float field."""
    value = payload.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{field} must be numeric when provided")
    return float(value)


def transaction_from_dict(payload: dict[str, Any]) -> Transaction:
    """Create a Transaction from a decoded JSON object."""
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dictionary")

    amount = payload.get("amount")
    if isinstance(amount, bool) or not isinstance(amount, int | float):
        raise ValueError("amount must be numeric")

    is_card_present = payload.get("is_card_present")
    if not isinstance(is_card_present, bool):
        raise ValueError("is_card_present must be boolean")

    return Transaction(
        transaction_id=_required_str(payload, "transaction_id"),
        user_id=_required_str(payload, "user_id"),
        card_id=_required_str(payload, "card_id"),
        merchant_id=_required_str(payload, "merchant_id"),
        amount=float(amount),
        currency=_required_str(payload, "currency").upper(),
        country=_required_str(payload, "country").upper(),
        device_id=_required_str(payload, "device_id"),
        merchant_category=_required_str(payload, "merchant_category"),
        event_time=parse_event_time(_required_str(payload, "event_time")),
        channel=_required_str(payload, "channel"),
        is_card_present=is_card_present,
        latitude=_optional_float(payload, "latitude"),
        longitude=_optional_float(payload, "longitude"),
    )


def transaction_from_json(line: str) -> Transaction:
    """Parse one JSON line into a Transaction."""
    if not isinstance(line, str):
        raise TypeError("line must be a string")
    payload = json.loads(line)
    if not isinstance(payload, dict):
        raise ValueError("transaction JSON must decode to an object")
    return transaction_from_dict(payload)


def transaction_to_json(transaction: Transaction) -> str:
    """Serialize a Transaction to compact JSON."""
    if not isinstance(transaction, Transaction):
        raise TypeError("transaction must be a Transaction")
    return json.dumps(transaction.to_dict(), separators=(",", ":"), sort_keys=True)


def _required_bool(payload: dict[str, Any], field: str) -> bool:
    """Read a required boolean field from a decoded JSON object."""
    value = payload.get(field)
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be boolean")
    return value


def _required_int(payload: dict[str, Any], field: str) -> int:
    """Read a required integer field from a decoded JSON object."""
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")
    return value


def _required_float(payload: dict[str, Any], field: str) -> float:
    """Read a required float-compatible field from a decoded JSON object."""
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{field} must be numeric")
    return float(value)


def _required_reasons(payload: dict[str, Any]) -> list[str]:
    """Read and validate the alert reasons list."""
    reasons = payload.get("reasons")
    if not isinstance(reasons, list) or not all(isinstance(item, str) for item in reasons):
        raise ValueError("reasons must be a list of strings")
    return reasons


def _required_risk_level(payload: dict[str, Any]) -> RiskLevel:
    """Read and validate the alert risk level."""
    value = payload.get("risk_level")
    if value not in {"low", "elevated", "medium", "high"}:
        raise ValueError("risk_level must be one of: low, elevated, medium, high")
    return cast(RiskLevel, value)


def _features_from_alert_dict(payload: dict[str, Any]) -> FraudFeatures:
    """Create embedded feature data from an alert payload."""
    features = payload.get("features")
    if not isinstance(features, dict):
        raise ValueError("features must be an object")

    event_time = parse_event_time(_required_str(payload, "event_time"))
    return FraudFeatures(
        transaction_id=_required_str(payload, "transaction_id"),
        user_id=_required_str(payload, "user_id"),
        card_id=_required_str(payload, "card_id"),
        event_time=event_time,
        amount=_required_float(features, "amount"),
        tx_count_5m=_required_int(features, "tx_count_5m"),
        amount_sum_1h=_required_float(features, "amount_sum_1h"),
        amount_zscore=_required_float(features, "amount_zscore"),
        minutes_since_last_tx=(
            None
            if features.get("minutes_since_last_tx") is None
            else _required_float(features, "minutes_since_last_tx")
        ),
        country_changed=_required_bool(features, "country_changed"),
        device_changed=_required_bool(features, "device_changed"),
        card_not_present=_required_bool(features, "card_not_present"),
        night_transaction=_required_bool(features, "night_transaction"),
        high_velocity=_required_bool(features, "high_velocity"),
        high_amount=_required_bool(features, "high_amount"),
    )


def alert_from_dict(payload: dict[str, Any]) -> Alert:
    """Create an Alert from a decoded JSON object."""
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dictionary")

    features = _features_from_alert_dict(payload)
    return Alert(
        transaction_id=features.transaction_id,
        user_id=features.user_id,
        card_id=features.card_id,
        event_time=features.event_time,
        risk_score=_required_int(payload, "risk_score"),
        risk_level=_required_risk_level(payload),
        reasons=_required_reasons(payload),
        features=features,
    )


def alert_from_json(line: str) -> Alert:
    """Parse one JSON line into an Alert."""
    if not isinstance(line, str):
        raise TypeError("line must be a string")
    payload = json.loads(line)
    if not isinstance(payload, dict):
        raise ValueError("alert JSON must decode to an object")
    return alert_from_dict(payload)


def alert_to_json(alert: Alert) -> str:
    """Serialize an Alert to compact JSON."""
    if not isinstance(alert, Alert):
        raise TypeError("alert must be an Alert")
    return json.dumps(alert.to_dict(), separators=(",", ":"), sort_keys=True)
