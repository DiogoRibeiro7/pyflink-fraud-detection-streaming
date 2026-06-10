"""JSON serialization helpers for stream records."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from fraud_streaming.schemas import Alert, Transaction


def parse_event_time(value: str) -> datetime:
    """Parse an ISO-8601 timestamp into a timezone-aware datetime."""
    if not isinstance(value, str):
        raise TypeError("event_time must be a string")
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError("event_time must include timezone information")
    return parsed.astimezone(UTC)


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


def alert_to_json(alert: Alert) -> str:
    """Serialize an Alert to compact JSON."""
    if not isinstance(alert, Alert):
        raise TypeError("alert must be an Alert")
    return json.dumps(alert.to_dict(), separators=(",", ":"), sort_keys=True)
