"""Event quality checks and dead-letter helpers for transaction streams."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from fraud_streaming.schemas import Transaction
from fraud_streaming.serialization import parse_event_time, transaction_from_dict

QualitySeverity = Literal["warning", "error"]

DEFAULT_SUPPORTED_CURRENCIES = frozenset({"EUR", "USD", "GBP"})
DEFAULT_FUTURE_EVENT_TOLERANCE = timedelta(minutes=5)

REQUIRED_TRANSACTION_FIELDS = (
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
)


@dataclass(frozen=True, slots=True)
class QualityCheckResult:
    """Structured result for one event quality check."""

    check_name: str
    severity: QualitySeverity
    passed: bool
    message: str
    field_name: str | None = None

    def to_dict(self) -> dict[str, str | bool | None]:
        """Return a JSON-compatible representation."""
        return {
            "check_name": self.check_name,
            "severity": self.severity,
            "passed": self.passed,
            "message": self.message,
            "field_name": self.field_name,
        }


@dataclass(frozen=True, slots=True)
class DeadLetterRecord:
    """Structured dead-letter representation for malformed events."""

    raw_event: str
    parse_error: str | None
    quality_failures: list[QualityCheckResult]
    ingestion_time: datetime

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible dictionary."""
        return {
            "raw_event": self.raw_event,
            "parse_error": self.parse_error,
            "quality_failures": [failure.to_dict() for failure in self.quality_failures],
            "ingestion_time": self.ingestion_time.astimezone(timezone.utc).isoformat(),
        }


@dataclass(frozen=True, slots=True)
class ValidatedEventResult:
    """Outcome of parsing and validating a raw transaction event."""

    transaction: Transaction | None
    dead_letter: DeadLetterRecord | None


def _result(
    *,
    check_name: str,
    passed: bool,
    message: str,
    field_name: str | None = None,
    severity: QualitySeverity = "error",
) -> QualityCheckResult:
    """Build a quality check result."""
    return QualityCheckResult(
        check_name=check_name,
        severity=severity,
        passed=passed,
        message=message,
        field_name=field_name,
    )


def _decode_transaction_payload(raw_event: str) -> dict[str, Any]:
    """Decode a raw JSON event into an object payload."""
    payload = json.loads(raw_event)
    if not isinstance(payload, dict):
        raise ValueError("transaction JSON must decode to an object")
    return payload


def run_quality_checks(
    payload: dict[str, Any],
    *,
    seen_transaction_ids: set[str] | None = None,
    current_time: datetime | None = None,
    future_tolerance: timedelta = DEFAULT_FUTURE_EVENT_TOLERANCE,
    supported_currencies: frozenset[str] = DEFAULT_SUPPORTED_CURRENCIES,
) -> list[QualityCheckResult]:
    """Run structured quality checks against a decoded transaction payload."""
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dictionary")

    reference_time = current_time or datetime.now(timezone.utc)
    results: list[QualityCheckResult] = []

    missing_fields = [field for field in REQUIRED_TRANSACTION_FIELDS if field not in payload]
    if missing_fields:
        for field in missing_fields:
            results.append(
                _result(
                    check_name="missing_required_field",
                    passed=False,
                    message=f"missing required field: {field}",
                    field_name=field,
                )
            )
    else:
        results.append(
            _result(
                check_name="missing_required_field",
                passed=True,
                message="all required fields are present",
            )
        )

    transaction_id = payload.get("transaction_id")
    if isinstance(transaction_id, str) and transaction_id:
        if seen_transaction_ids is not None and transaction_id in seen_transaction_ids:
            results.append(
                _result(
                    check_name="duplicate_transaction_id",
                    passed=False,
                    message=f"duplicate transaction_id detected in local batch: {transaction_id}",
                    field_name="transaction_id",
                )
            )
        else:
            results.append(
                _result(
                    check_name="duplicate_transaction_id",
                    passed=True,
                    message="transaction_id is unique in the current batch",
                    field_name="transaction_id",
                )
            )

    for field_name in ("user_id", "card_id"):
        value = payload.get(field_name)
        if isinstance(value, str) and value.strip():
            results.append(
                _result(
                    check_name="non_empty_identifier",
                    passed=True,
                    message=f"{field_name} is non-empty",
                    field_name=field_name,
                )
            )
        else:
            results.append(
                _result(
                    check_name="non_empty_identifier",
                    passed=False,
                    message=f"{field_name} must be a non-empty string",
                    field_name=field_name,
                )
            )

    amount = payload.get("amount")
    if isinstance(amount, bool) or not isinstance(amount, int | float):
        results.append(
            _result(
                check_name="valid_amount",
                passed=False,
                message="amount must be numeric",
                field_name="amount",
            )
        )
    elif float(amount) < 0:
        results.append(
            _result(
                check_name="valid_amount",
                passed=False,
                message="amount cannot be negative",
                field_name="amount",
            )
        )
    else:
        results.append(
            _result(
                check_name="valid_amount",
                passed=True,
                message="amount is valid",
                field_name="amount",
            )
        )

    currency = payload.get("currency")
    if not isinstance(currency, str) or not currency.strip():
        results.append(
            _result(
                check_name="supported_currency",
                passed=False,
                message="currency must be a non-empty string",
                field_name="currency",
            )
        )
    elif currency.upper() not in supported_currencies:
        results.append(
            _result(
                check_name="supported_currency",
                passed=False,
                message=f"unsupported currency: {currency}",
                field_name="currency",
            )
        )
    else:
        results.append(
            _result(
                check_name="supported_currency",
                passed=True,
                message=f"currency {currency.upper()} is supported",
                field_name="currency",
            )
        )

    event_time_value = payload.get("event_time")
    try:
        if not isinstance(event_time_value, str):
            raise TypeError("event_time must be a string")
        parsed_event_time = parse_event_time(event_time_value)
    except (TypeError, ValueError) as exc:
        results.append(
            _result(
                check_name="valid_timestamp",
                passed=False,
                message=str(exc),
                field_name="event_time",
            )
        )
    else:
        results.append(
            _result(
                check_name="valid_timestamp",
                passed=True,
                message="event_time is valid",
                field_name="event_time",
            )
        )
        if parsed_event_time > reference_time + future_tolerance:
            results.append(
                _result(
                    check_name="future_event_time",
                    passed=False,
                    message="event_time is beyond the allowed future tolerance",
                    field_name="event_time",
                )
            )
        else:
            results.append(
                _result(
                    check_name="future_event_time",
                    passed=True,
                    message="event_time is within the allowed future tolerance",
                    field_name="event_time",
                )
            )

    return results


def quality_failures(results: list[QualityCheckResult]) -> list[QualityCheckResult]:
    """Return the failing quality checks from a result list."""
    return [result for result in results if not result.passed]


def build_dead_letter_record(
    *,
    raw_event: str,
    parse_error: str | None,
    quality_failures: list[QualityCheckResult],
    ingestion_time: datetime | None = None,
) -> DeadLetterRecord:
    """Create a dead-letter record for a malformed event."""
    return DeadLetterRecord(
        raw_event=raw_event,
        parse_error=parse_error,
        quality_failures=quality_failures,
        ingestion_time=ingestion_time or datetime.now(timezone.utc),
    )


def dead_letter_to_json(record: DeadLetterRecord) -> str:
    """Serialize a dead-letter record to compact JSON."""
    return json.dumps(record.to_dict(), separators=(",", ":"), sort_keys=True)


def parse_and_validate_transaction_event(
    raw_event: str,
    *,
    seen_transaction_ids: set[str] | None = None,
    current_time: datetime | None = None,
    future_tolerance: timedelta = DEFAULT_FUTURE_EVENT_TOLERANCE,
    supported_currencies: frozenset[str] = DEFAULT_SUPPORTED_CURRENCIES,
) -> ValidatedEventResult:
    """Parse and validate a raw transaction event.

    This helper is intentionally runtime-agnostic so it can be reused by the
    local runner today and by future PyFlink/Kafka dead-letter routing logic.
    """
    try:
        payload = _decode_transaction_payload(raw_event)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        return ValidatedEventResult(
            transaction=None,
            dead_letter=build_dead_letter_record(
                raw_event=raw_event,
                parse_error=str(exc),
                quality_failures=[],
            ),
        )

    results = run_quality_checks(
        payload,
        seen_transaction_ids=seen_transaction_ids,
        current_time=current_time,
        future_tolerance=future_tolerance,
        supported_currencies=supported_currencies,
    )
    failures = quality_failures(results)
    if failures:
        return ValidatedEventResult(
            transaction=None,
            dead_letter=build_dead_letter_record(
                raw_event=raw_event,
                parse_error=None,
                quality_failures=failures,
            ),
        )

    transaction = transaction_from_dict(payload)
    if seen_transaction_ids is not None:
        seen_transaction_ids.add(transaction.transaction_id)
    return ValidatedEventResult(transaction=transaction, dead_letter=None)
