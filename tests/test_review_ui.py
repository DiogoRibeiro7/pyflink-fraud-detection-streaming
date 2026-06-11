from __future__ import annotations

from datetime import datetime, timezone

from fraud_streaming.features import compute_features
from fraud_streaming.review_ui import build_review_rows, render_review_ui_html
from fraud_streaming.rules import build_alert, score_features
from fraud_streaming.schemas import AnalystFeedback, Transaction, UserProfileState


def make_transaction(transaction_id: str, amount: float) -> Transaction:
    return Transaction(
        transaction_id=transaction_id,
        user_id="user-1",
        card_id="card-1",
        merchant_id="merchant-1",
        amount=amount,
        currency="EUR",
        country="PT",
        device_id="device-1",
        merchant_category="grocery",
        event_time=datetime(2026, 6, 10, 12, 0, tzinfo=timezone.utc),
        channel="pos",
        is_card_present=True,
    )


def make_alert(transaction_id: str, amount: float) -> object:
    transaction = make_transaction(transaction_id, amount)
    features = compute_features(transaction, UserProfileState())
    return build_alert(features, score_features(features))


def test_build_review_rows_includes_latest_feedback() -> None:
    alert = make_alert("tx-1", 42.0)
    feedback = AnalystFeedback(
        transaction_id="tx-1",
        reviewer_id="analyst-1",
        label="true_fraud",
        comment="confirmed",
        reviewed_at=datetime(2026, 6, 10, 13, 0, tzinfo=timezone.utc),
    )

    rows = build_review_rows([alert], [feedback])

    assert rows[0].transaction_id == "tx-1"
    assert rows[0].current_feedback is not None
    assert rows[0].current_feedback["label"] == "true_fraud"


def test_render_review_ui_html_embeds_rows_and_export_flow() -> None:
    html = render_review_ui_html(
        build_review_rows([make_alert("tx-1", 42.0)], []),
        "Analyst Review Demo",
    )

    assert "Analyst Review Demo" in html
    assert "tx-1" in html
    assert "Download Feedback JSONL" in html
    assert "fraud-feedback-report" in html
