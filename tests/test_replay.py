from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from fraud_streaming.replay import (
    ReplayPlanItem,
    execute_replay,
    filter_transactions,
    load_transactions,
    plan_replay,
    validate_args,
)
from fraud_streaming.schemas import Transaction


class RecordingSink:
    def __init__(self) -> None:
        self.transactions: list[Transaction] = []
        self.closed = False

    def emit(self, transaction: Transaction) -> None:
        self.transactions.append(transaction)

    def close(self) -> None:
        self.closed = True


def make_transaction(transaction_id: str, minutes: int, user_id: str = "user-1") -> Transaction:
    return Transaction(
        transaction_id=transaction_id,
        user_id=user_id,
        card_id="card-1",
        merchant_id="merchant-1",
        amount=20.0,
        currency="EUR",
        country="PT",
        device_id="device-1",
        merchant_category="grocery",
        event_time=datetime(2026, 6, 10, 12, minutes, tzinfo=timezone.utc),
        channel="pos",
        is_card_present=True,
    )


def test_filter_transactions_applies_time_range_and_user_filter() -> None:
    transactions = [
        make_transaction("tx-1", 0, user_id="user-1"),
        make_transaction("tx-2", 5, user_id="user-2"),
        make_transaction("tx-3", 10, user_id="user-1"),
    ]

    filtered = filter_transactions(
        transactions,
        start_time="2026-06-10T12:04:00Z",
        end_time="2026-06-10T12:10:00Z",
        user_id="user-1",
    )

    assert [transaction.transaction_id for transaction in filtered] == ["tx-3"]


def test_plan_replay_computes_speed_multiplier_delays() -> None:
    transactions = [
        make_transaction("tx-1", 0),
        make_transaction("tx-2", 5),
        make_transaction("tx-3", 7),
    ]

    plan = plan_replay(transactions, speed_multiplier=2.0)

    assert [item.delay_seconds for item in plan] == [0.0, 150.0, 60.0]


def test_plan_replay_as_fast_as_possible_zeroes_delays() -> None:
    transactions = [make_transaction("tx-1", 0), make_transaction("tx-2", 5)]

    plan = plan_replay(transactions, as_fast_as_possible=True)

    assert all(item.delay_seconds == 0.0 for item in plan)


def test_execute_replay_uses_sleep_function_without_real_sleep() -> None:
    sink = RecordingSink()
    sleeps: list[float] = []
    plan = [
        ReplayPlanItem(transaction=make_transaction("tx-1", 0), delay_seconds=0.0),
        ReplayPlanItem(transaction=make_transaction("tx-2", 1), delay_seconds=3.0),
    ]

    count = execute_replay(plan, sink, sleep_fn=sleeps.append)

    assert count == 2
    assert sleeps == [3.0]
    assert [transaction.transaction_id for transaction in sink.transactions] == ["tx-1", "tx-2"]
    assert sink.closed is True


def test_validate_args_rejects_missing_file(tmp_path: Path) -> None:
    args = argparse.Namespace(
        input=tmp_path / "missing.jsonl",
        output_mode="stdout",
        output_path=None,
        bootstrap_servers=None,
        topic=None,
        key_field="user_id",
        preserve_timing=False,
        speed_multiplier=None,
        as_fast_as_possible=False,
        start_time=None,
        end_time=None,
        user_id=None,
        card_id=None,
        dry_run=False,
    )

    try:
        validate_args(args)
    except ValueError as exc:
        assert "does not exist" in str(exc)
    else:
        raise AssertionError("expected ValueError for missing input file")


def test_load_transactions_preserves_order(tmp_path: Path) -> None:
    input_path = tmp_path / "transactions.jsonl"
    input_path.write_text(
        "\n".join(
            [
                '{"transaction_id":"tx-1","user_id":"user-1","card_id":"card-1","merchant_id":"merchant-1","amount":20.0,"currency":"EUR","country":"PT","device_id":"device-1","merchant_category":"grocery","event_time":"2026-06-10T12:00:00Z","channel":"pos","is_card_present":true}',
                '{"transaction_id":"tx-2","user_id":"user-1","card_id":"card-1","merchant_id":"merchant-1","amount":21.0,"currency":"EUR","country":"PT","device_id":"device-1","merchant_category":"grocery","event_time":"2026-06-10T12:01:00Z","channel":"pos","is_card_present":true}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    transactions = load_transactions(input_path)

    assert [transaction.transaction_id for transaction in transactions] == ["tx-1", "tx-2"]
