from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from fraud_streaming.kafka.producer import (
    VALID_KEY_FIELDS,
    iter_generated_transactions,
    iter_transactions_from_file,
    prepare_message,
    validate_args,
)
from fraud_streaming.schemas import Transaction


def make_transaction() -> Transaction:
    return Transaction(
        transaction_id="tx-1",
        user_id="user-1",
        card_id="card-1",
        merchant_id="merchant-1",
        amount=42.0,
        currency="EUR",
        country="PT",
        device_id="device-1",
        merchant_category="grocery",
        event_time=datetime(2026, 6, 10, 12, 0, tzinfo=timezone.utc),
        channel="pos",
        is_card_present=True,
    )


def test_validate_args_rejects_unknown_key_field(tmp_path: Path) -> None:
    args = argparse.Namespace(
        bootstrap_servers="localhost:9092",
        topic="transactions",
        input=tmp_path / "transactions.jsonl",
        users=10,
        transactions=100,
        seed=42,
        sleep_ms=0,
        key_field="unknown",
    )
    args.input.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="--key-field must be one of"):
        validate_args(args)


def test_validate_args_requires_positive_generated_counts() -> None:
    args = argparse.Namespace(
        bootstrap_servers="localhost:9092",
        topic="transactions",
        input=None,
        users=0,
        transactions=100,
        seed=42,
        sleep_ms=0,
        key_field="user_id",
    )

    with pytest.raises(ValueError, match="--users must be positive"):
        validate_args(args)


def test_iter_transactions_from_file_reports_line_number(tmp_path: Path) -> None:
    bad_file = tmp_path / "bad.jsonl"
    bad_file.write_text('{"transaction_id":"tx-1"}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="line 1"):
        list(iter_transactions_from_file(bad_file))


def test_prepare_message_uses_canonical_json_and_key_field() -> None:
    transaction = make_transaction()

    message = prepare_message(transaction, "user_id")

    assert message.key == b"user-1"
    payload = json.loads(message.value.decode("utf-8"))
    assert payload["transaction_id"] == "tx-1"
    assert payload["currency"] == "EUR"
    assert payload["event_time"] == "2026-06-10T12:00:00Z"


def test_prepare_message_rejects_unsupported_key_field() -> None:
    transaction = make_transaction()

    with pytest.raises(ValueError, match="unsupported key field"):
        prepare_message(transaction, "unsupported")


def test_generated_transactions_are_valid_transactions() -> None:
    generated = list(iter_generated_transactions(users=2, transactions=3, seed=7))

    assert len(generated) == 3
    assert all(isinstance(item, Transaction) for item in generated)
    assert "user_id" in VALID_KEY_FIELDS
