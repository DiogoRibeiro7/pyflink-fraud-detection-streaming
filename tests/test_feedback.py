from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from fraud_streaming.features import compute_features, update_state
from fraud_streaming.feedback import (
    build_feedback_report,
    build_retraining_export,
    feedback_from_json,
    join_alerts_with_feedback,
    load_feedback,
    summarize_false_positive_rate_by_reason,
    summarize_precision_by_risk_level,
)
from fraud_streaming.rules import build_alert, score_features
from fraud_streaming.schemas import Alert, AnalystFeedback, Transaction, UserProfileState


def make_transaction(
    transaction_id: str,
    *,
    user_id: str = "user-1",
    amount: float = 50.0,
    minutes: int = 0,
    country: str = "PT",
    device_id: str = "device-1",
    merchant_category: str = "grocery",
    channel: str = "pos",
    is_card_present: bool = True,
) -> Transaction:
    return Transaction(
        transaction_id=transaction_id,
        user_id=user_id,
        card_id="card-1",
        merchant_id="merchant-1",
        amount=amount,
        currency="EUR",
        country=country,
        device_id=device_id,
        merchant_category=merchant_category,
        event_time=datetime(2026, 6, 10, 12, minutes, tzinfo=timezone.utc),
        channel=channel,
        is_card_present=is_card_present,
    )


def build_alerts(transactions: list[Transaction]) -> list[Alert]:
    states: dict[str, UserProfileState] = {}
    alerts: list[Alert] = []
    for transaction in transactions:
        state = states.get(transaction.key, UserProfileState())
        features = compute_features(transaction, state)
        alerts.append(build_alert(features, score_features(features)))
        states[transaction.key] = update_state(transaction, state)
    return alerts


def make_feedback(
    transaction_id: str,
    label: str,
    *,
    reviewer_id: str = "analyst-1",
    minute: int = 30,
    comment: str = "",
) -> AnalystFeedback:
    return AnalystFeedback(
        transaction_id=transaction_id,
        reviewer_id=reviewer_id,
        label=label,  # type: ignore[arg-type]
        comment=comment,
        reviewed_at=datetime(2026, 6, 10, 13, minute, tzinfo=timezone.utc),
    )


def test_feedback_from_json_validates_required_fields() -> None:
    payload = json.dumps(
        {
            "transaction_id": "tx-1",
            "reviewer_id": "analyst-1",
            "label": "true_fraud",
            "comment": "confirmed",
            "reviewed_at": "2026-06-10T13:00:00Z",
        }
    )

    feedback = feedback_from_json(payload)

    assert feedback.transaction_id == "tx-1"
    assert feedback.label == "true_fraud"


def test_feedback_from_json_rejects_invalid_label() -> None:
    payload = json.dumps(
        {
            "transaction_id": "tx-1",
            "reviewer_id": "analyst-1",
            "label": "fraud",
            "reviewed_at": "2026-06-10T13:00:00Z",
        }
    )

    with pytest.raises(ValueError, match="label must be one of"):
        feedback_from_json(payload)


def test_load_feedback_rejects_duplicate_review_keys(tmp_path: Path) -> None:
    path = tmp_path / "feedback.jsonl"
    lines = [
        {
            "transaction_id": "tx-1",
            "reviewer_id": "analyst-1",
            "label": "true_fraud",
            "reviewed_at": "2026-06-10T13:00:00Z",
        },
        {
            "transaction_id": "tx-1",
            "reviewer_id": "analyst-1",
            "label": "false_positive",
            "reviewed_at": "2026-06-10T13:00:00Z",
        },
    ]
    path.write_text("\n".join(json.dumps(line) for line in lines), encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate feedback review detected"):
        load_feedback(path)


def test_join_alerts_with_feedback_uses_latest_review() -> None:
    alerts = build_alerts([make_transaction("tx-1")])
    older = make_feedback("tx-1", "false_positive", minute=1)
    newer = make_feedback("tx-1", "true_fraud", minute=2)

    joined, unmatched = join_alerts_with_feedback(alerts, [older, newer])

    assert len(unmatched) == 0
    assert joined[0].feedback is not None
    assert joined[0].feedback.label == "true_fraud"


def test_feedback_report_summarizes_precision_and_unmatched_feedback() -> None:
    transactions = [
        make_transaction("tx-1"),
        make_transaction("tx-2", amount=1200.0, is_card_present=False, minutes=1),
    ]
    alerts = build_alerts(transactions)
    feedback_rows = [
        make_feedback("tx-1", "false_positive"),
        make_feedback("tx-2", "true_fraud"),
        make_feedback("tx-missing", "needs_review"),
    ]

    report, retraining_export = build_feedback_report(
        alerts, feedback_rows, transactions=transactions
    )

    assert retraining_export is not None
    assert report.counts.total_alerts == 2
    assert report.counts.reviewed_alerts == 2
    assert report.counts.unreviewed_alerts == 0
    assert report.counts.feedback_without_alert == 1
    assert report.unmatched_feedback_transaction_ids == ["tx-missing"]
    shared_risk_level = alerts[0].risk_level
    precision_by_key = {row.key: row.precision for row in report.precision_by_risk_level}

    assert precision_by_key[shared_risk_level] == 0.5


def test_reason_and_risk_level_summaries_use_reviewed_alerts_only() -> None:
    transactions = [
        make_transaction("tx-1"),
        make_transaction(
            "tx-2",
            amount=1500.0,
            minutes=1,
            country="ES",
            device_id="device-2",
            is_card_present=False,
        ),
    ]
    alerts = build_alerts(transactions)
    joined, _ = join_alerts_with_feedback(
        alerts,
        [
            make_feedback("tx-1", "false_positive"),
            make_feedback("tx-2", "true_fraud"),
        ],
    )

    precision_rows = summarize_precision_by_risk_level(joined)
    reason_rows = summarize_false_positive_rate_by_reason(joined)

    low_row = next(row for row in precision_rows if row.key == "low")
    assert low_row.reviewed_alerts == 1
    assert low_row.precision == 0.0

    assert any(row.false_positive_rate == 0.0 for row in reason_rows)


def test_retraining_export_reports_unmatched_reviewed_alerts() -> None:
    transactions = [make_transaction("tx-1")]
    alerts = build_alerts(
        [
            make_transaction("tx-1"),
            make_transaction("tx-2", amount=900.0, minutes=1),
        ]
    )
    joined, _ = join_alerts_with_feedback(
        alerts,
        [
            make_feedback("tx-1", "false_positive"),
            make_feedback("tx-2", "true_fraud"),
        ],
    )

    export = build_retraining_export(transactions, joined)

    assert len(export.rows) == 1
    assert export.rows[0].transaction_id == "tx-1"
    assert export.rows[0].label == 0
    assert export.rows[0].label_source == "analyst_feedback"
    assert tuple(export.rows[0].feature_values) != ()
    assert export.unmatched_transaction_count == 1
