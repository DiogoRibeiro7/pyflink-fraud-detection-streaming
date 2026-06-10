from __future__ import annotations

import io
import json

import pytest

from fraud_streaming.local_runner import process_json_lines


def test_process_json_lines_writes_dead_letters_and_continues() -> None:
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
        json.dumps(
            {
                "transaction_id": "tx-2",
                "user_id": "user-2",
                "card_id": "card-2",
                "merchant_id": "merchant-2",
                "amount": 55.0,
                "currency": "EUR",
                "country": "PT",
                "device_id": "device-2",
                "merchant_category": "fuel",
                "event_time": "2026-06-10T12:02:00Z",
                "channel": "pos",
                "is_card_present": True,
            }
        ),
    ]
    dead_letters = io.StringIO()

    alerts = list(process_json_lines(lines, emit_low_risk=True, dead_letter_handle=dead_letters))

    assert len(alerts) == 2
    payload = json.loads(dead_letters.getvalue().strip())
    assert payload["parse_error"] is not None


def test_process_json_lines_raises_without_dead_letter_handle() -> None:
    with pytest.raises(ValueError):
        list(process_json_lines(["{bad json"], emit_low_risk=True))
