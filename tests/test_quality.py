from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from fraud_streaming.quality import (
    build_dead_letter_record,
    dead_letter_to_json,
    parse_and_validate_transaction_event,
    quality_failures,
    run_quality_checks,
)


def make_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
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
    payload.update(overrides)
    return payload


def test_run_quality_checks_accepts_valid_payload() -> None:
    results = run_quality_checks(
        make_payload(),
        current_time=datetime(2026, 6, 10, 12, 2, tzinfo=timezone.utc),
    )

    assert quality_failures(results) == []


def test_run_quality_checks_flags_missing_required_fields() -> None:
    payload = make_payload()
    del payload["merchant_id"]

    failures = quality_failures(
        run_quality_checks(
            payload,
            current_time=datetime(2026, 6, 10, 12, 2, tzinfo=timezone.utc),
        )
    )

    assert any(failure.check_name == "missing_required_field" for failure in failures)


def test_run_quality_checks_flags_invalid_amount() -> None:
    failures = quality_failures(
        run_quality_checks(
            make_payload(amount=-5.0),
            current_time=datetime(2026, 6, 10, 12, 2, tzinfo=timezone.utc),
        )
    )

    assert any(failure.check_name == "valid_amount" for failure in failures)


def test_run_quality_checks_flags_invalid_timestamp() -> None:
    failures = quality_failures(
        run_quality_checks(
            make_payload(event_time="not-a-timestamp"),
            current_time=datetime(2026, 6, 10, 12, 2, tzinfo=timezone.utc),
        )
    )

    assert any(failure.check_name == "valid_timestamp" for failure in failures)


def test_run_quality_checks_flags_unsupported_currency() -> None:
    failures = quality_failures(
        run_quality_checks(
            make_payload(currency="BTC"),
            current_time=datetime(2026, 6, 10, 12, 2, tzinfo=timezone.utc),
        )
    )

    assert any(failure.check_name == "supported_currency" for failure in failures)


def test_run_quality_checks_flags_empty_identifiers() -> None:
    failures = quality_failures(
        run_quality_checks(
            make_payload(user_id="", card_id=" "),
            current_time=datetime(2026, 6, 10, 12, 2, tzinfo=timezone.utc),
        )
    )

    identifier_failures = [
        failure for failure in failures if failure.check_name == "non_empty_identifier"
    ]
    assert len(identifier_failures) == 2


def test_run_quality_checks_flags_future_event_time() -> None:
    failures = quality_failures(
        run_quality_checks(
            make_payload(event_time="2026-06-10T12:30:00Z"),
            current_time=datetime(2026, 6, 10, 12, 0, tzinfo=timezone.utc),
            future_tolerance=timedelta(minutes=5),
        )
    )

    assert any(failure.check_name == "future_event_time" for failure in failures)


def test_run_quality_checks_flags_duplicate_transaction_ids() -> None:
    failures = quality_failures(
        run_quality_checks(
            make_payload(transaction_id="tx-duplicate"),
            seen_transaction_ids={"tx-duplicate"},
            current_time=datetime(2026, 6, 10, 12, 2, tzinfo=timezone.utc),
        )
    )

    assert any(failure.check_name == "duplicate_transaction_id" for failure in failures)


def test_parse_and_validate_transaction_event_returns_dead_letter_for_bad_json() -> None:
    validated = parse_and_validate_transaction_event("{bad json")

    assert validated.transaction is None
    assert validated.dead_letter is not None
    assert validated.dead_letter.parse_error is not None


def test_dead_letter_to_json_preserves_failure_details() -> None:
    failure = quality_failures(
        run_quality_checks(
            make_payload(currency="BTC"),
            current_time=datetime(2026, 6, 10, 12, 2, tzinfo=timezone.utc),
        )
    )
    record = build_dead_letter_record(
        raw_event=json.dumps(make_payload(currency="BTC")),
        parse_error=None,
        quality_failures=failure,
        ingestion_time=datetime(2026, 6, 10, 12, 3, tzinfo=timezone.utc),
    )

    payload = json.loads(dead_letter_to_json(record))

    assert payload["parse_error"] is None
    assert payload["quality_failures"][0]["check_name"] == "supported_currency"
