"""Offline analyst feedback ingestion and reporting utilities."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from fraud_streaming.features import compute_features, update_state
from fraud_streaming.ml.training import build_feature_dict
from fraud_streaming.schemas import (
    Alert,
    AnalystFeedback,
    AnalystLabel,
    Transaction,
    UserProfileState,
)
from fraud_streaming.serialization import alert_from_json, parse_event_time, transaction_from_dict


@dataclass(frozen=True, slots=True)
class JoinedAlertFeedback:
    """Alert paired with optional analyst feedback."""

    alert: Alert
    feedback: AnalystFeedback | None


@dataclass(frozen=True, slots=True)
class FeedbackSummaryRow:
    """One aggregate row for a feedback summary table."""

    key: str
    reviewed_alerts: int
    true_fraud: int
    false_positive: int
    needs_review: int
    precision: float | None = None
    false_positive_rate: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "key": self.key,
            "reviewed_alerts": self.reviewed_alerts,
            "true_fraud": self.true_fraud,
            "false_positive": self.false_positive,
            "needs_review": self.needs_review,
            "precision": self.precision,
            "false_positive_rate": self.false_positive_rate,
        }


@dataclass(frozen=True, slots=True)
class FeedbackCounts:
    """High-level reviewed versus unreviewed counts."""

    total_alerts: int
    reviewed_alerts: int
    unreviewed_alerts: int
    feedback_without_alert: int
    label_counts: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "total_alerts": self.total_alerts,
            "reviewed_alerts": self.reviewed_alerts,
            "unreviewed_alerts": self.unreviewed_alerts,
            "feedback_without_alert": self.feedback_without_alert,
            "label_counts": self.label_counts,
        }


@dataclass(frozen=True, slots=True)
class RetrainingExportRow:
    """One retraining dataset row built from transactions, alerts, and feedback."""

    transaction_id: str
    user_id: str
    card_id: str
    event_time: str
    reviewer_id: str
    analyst_label: AnalystLabel
    reviewed_at: str
    risk_score: int
    risk_level: str
    reasons: list[str]
    feature_values: dict[str, float | str]
    label: int | None
    label_source: str
    review_comment: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "transaction_id": self.transaction_id,
            "user_id": self.user_id,
            "card_id": self.card_id,
            "event_time": self.event_time,
            "reviewer_id": self.reviewer_id,
            "analyst_label": self.analyst_label,
            "reviewed_at": self.reviewed_at,
            "risk_score": self.risk_score,
            "risk_level": self.risk_level,
            "reasons": self.reasons,
            "feature_values": self.feature_values,
            "label": self.label,
            "label_source": self.label_source,
            "review_comment": self.review_comment,
        }


@dataclass(frozen=True, slots=True)
class RetrainingExportResult:
    """Retraining export rows plus unmatched-row counts."""

    rows: list[RetrainingExportRow]
    unmatched_feedback_count: int
    unmatched_transaction_count: int

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "row_count": len(self.rows),
            "unmatched_feedback_count": self.unmatched_feedback_count,
            "unmatched_transaction_count": self.unmatched_transaction_count,
        }


@dataclass(frozen=True, slots=True)
class FeedbackReport:
    """Full offline analyst feedback report."""

    counts: FeedbackCounts
    precision_by_risk_level: list[FeedbackSummaryRow]
    false_positive_rate_by_reason: list[FeedbackSummaryRow]
    unmatched_feedback_transaction_ids: list[str]
    retraining_export: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "counts": self.counts.to_dict(),
            "precision_by_risk_level": [row.to_dict() for row in self.precision_by_risk_level],
            "false_positive_rate_by_reason": [
                row.to_dict() for row in self.false_positive_rate_by_reason
            ],
            "unmatched_feedback_transaction_ids": self.unmatched_feedback_transaction_ids,
            "retraining_export": self.retraining_export,
        }


def build_parser() -> argparse.ArgumentParser:
    """Create the analyst feedback CLI parser."""
    parser = argparse.ArgumentParser(description="Build offline analyst feedback reports.")
    parser.add_argument("--alerts", type=Path, required=True, help="Alert JSONL input file.")
    parser.add_argument(
        "--feedback",
        type=Path,
        required=True,
        help="Analyst feedback JSONL input file.",
    )
    parser.add_argument(
        "--transactions",
        type=Path,
        help="Optional transaction JSONL file used for retraining export.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path where the JSON report will be written.",
    )
    parser.add_argument(
        "--retraining-output",
        type=Path,
        help="Optional JSONL export path for reviewed rows used in retraining.",
    )
    return parser


def validate_args(args: argparse.Namespace) -> None:
    """Validate CLI arguments for the feedback report."""
    for path_name in ("alerts", "feedback"):
        path = cast(Path, getattr(args, path_name))
        if not path.exists():
            raise ValueError(f"{path_name} file does not exist: {path}")
        if not path.is_file():
            raise ValueError(f"{path_name} path is not a file: {path}")

    if args.transactions is None and args.retraining_output is not None:
        raise ValueError("--transactions is required when --retraining-output is set")

    if args.transactions is not None:
        if not args.transactions.exists():
            raise ValueError(f"transactions file does not exist: {args.transactions}")
        if not args.transactions.is_file():
            raise ValueError(f"transactions path is not a file: {args.transactions}")

    if args.output.parent != Path():
        args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.retraining_output is not None and args.retraining_output.parent != Path():
        args.retraining_output.parent.mkdir(parents=True, exist_ok=True)


def _load_json_objects(path: Path, *, kind: str) -> list[dict[str, Any]]:
    """Load a JSONL file into decoded object payloads."""
    payloads: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError(f"{kind} JSON must decode to an object at line {line_number}")
            payloads.append(payload)
    if not payloads:
        raise ValueError(f"{kind} file is empty: {path}")
    return payloads


def _required_str(payload: dict[str, Any], field: str) -> str:
    """Read a required non-empty string from a decoded JSON object."""
    value = payload.get(field)
    if not isinstance(value, str) or value == "":
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _optional_str(payload: dict[str, Any], field: str) -> str | None:
    """Read an optional string from a decoded JSON object."""
    value = payload.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string when provided")
    return value


def _required_label(payload: dict[str, Any]) -> AnalystLabel:
    """Validate the analyst label field."""
    value = payload.get("label")
    if value not in {"true_fraud", "false_positive", "needs_review"}:
        raise ValueError("label must be one of: true_fraud, false_positive, needs_review")
    return cast(AnalystLabel, value)


def feedback_from_dict(payload: dict[str, Any]) -> AnalystFeedback:
    """Create an AnalystFeedback record from a decoded JSON object."""
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dictionary")

    return AnalystFeedback(
        transaction_id=_required_str(payload, "transaction_id"),
        reviewer_id=_required_str(payload, "reviewer_id"),
        label=_required_label(payload),
        comment=_optional_str(payload, "comment") or "",
        reviewed_at=parse_event_time(_required_str(payload, "reviewed_at")),
        alert_id=_optional_str(payload, "alert_id"),
    )


def feedback_from_json(line: str) -> AnalystFeedback:
    """Parse one JSON line into AnalystFeedback."""
    if not isinstance(line, str):
        raise TypeError("line must be a string")
    payload = json.loads(line)
    if not isinstance(payload, dict):
        raise ValueError("feedback JSON must decode to an object")
    return feedback_from_dict(payload)


def feedback_to_json(feedback: AnalystFeedback) -> str:
    """Serialize AnalystFeedback to compact JSON."""
    if not isinstance(feedback, AnalystFeedback):
        raise TypeError("feedback must be an AnalystFeedback")
    return json.dumps(feedback.to_dict(), separators=(",", ":"), sort_keys=True)


def load_alerts(path: Path) -> list[Alert]:
    """Load canonical alerts from a JSONL file."""
    payloads = _load_json_objects(path, kind="alert")
    return [alert_from_json(json.dumps(payload)) for payload in payloads]


def load_feedback(path: Path) -> list[AnalystFeedback]:
    """Load analyst feedback from a JSONL file."""
    payloads = _load_json_objects(path, kind="feedback")
    feedback_rows = [feedback_from_dict(payload) for payload in payloads]
    seen_review_keys: set[tuple[str, str, str]] = set()
    for row in feedback_rows:
        review_key = (row.transaction_id, row.reviewer_id, row.reviewed_at.isoformat())
        if review_key in seen_review_keys:
            raise ValueError(
                "duplicate feedback review detected for "
                f"transaction_id={row.transaction_id}, reviewer_id={row.reviewer_id}, "
                f"reviewed_at={row.reviewed_at.isoformat()}"
            )
        seen_review_keys.add(review_key)
    return feedback_rows


def join_alerts_with_feedback(
    alerts: Iterable[Alert],
    feedback_rows: Iterable[AnalystFeedback],
) -> tuple[list[JoinedAlertFeedback], list[AnalystFeedback]]:
    """Join alerts with the latest feedback entry per transaction."""
    alert_map = {alert.transaction_id: alert for alert in alerts}
    latest_feedback: dict[str, AnalystFeedback] = {}

    for row in feedback_rows:
        current = latest_feedback.get(row.transaction_id)
        if current is None or row.reviewed_at > current.reviewed_at:
            latest_feedback[row.transaction_id] = row

    joined = [
        JoinedAlertFeedback(alert=alert, feedback=latest_feedback.get(alert.transaction_id))
        for alert in alert_map.values()
    ]
    unmatched_feedback = [
        row for transaction_id, row in latest_feedback.items() if transaction_id not in alert_map
    ]
    return joined, unmatched_feedback


def build_review_counts(
    joined_rows: Iterable[JoinedAlertFeedback],
    unmatched_feedback: Iterable[AnalystFeedback],
) -> FeedbackCounts:
    """Build reviewed versus unreviewed counts."""
    joined_list = list(joined_rows)
    label_counter = Counter[str]()
    reviewed_alerts = 0
    for row in joined_list:
        if row.feedback is not None:
            reviewed_alerts += 1
            label_counter[row.feedback.label] += 1

    unmatched_feedback_list = list(unmatched_feedback)
    for feedback_row in unmatched_feedback_list:
        label_counter[feedback_row.label] += 1

    total_alerts = len(joined_list)
    return FeedbackCounts(
        total_alerts=total_alerts,
        reviewed_alerts=reviewed_alerts,
        unreviewed_alerts=total_alerts - reviewed_alerts,
        feedback_without_alert=len(unmatched_feedback_list),
        label_counts=dict(sorted(label_counter.items())),
    )


def summarize_precision_by_risk_level(
    joined_rows: Iterable[JoinedAlertFeedback],
) -> list[FeedbackSummaryRow]:
    """Compute reviewed precision by alert risk level."""
    grouped: dict[str, list[AnalystFeedback]] = defaultdict(list)
    for row in joined_rows:
        if row.feedback is not None:
            grouped[row.alert.risk_level].append(row.feedback)

    rows: list[FeedbackSummaryRow] = []
    for risk_level in ("low", "elevated", "medium", "high"):
        feedback_rows = grouped.get(risk_level, [])
        true_fraud = sum(row.label == "true_fraud" for row in feedback_rows)
        false_positive = sum(row.label == "false_positive" for row in feedback_rows)
        needs_review = sum(row.label == "needs_review" for row in feedback_rows)
        decisive = true_fraud + false_positive
        precision = true_fraud / decisive if decisive else None
        rows.append(
            FeedbackSummaryRow(
                key=risk_level,
                reviewed_alerts=len(feedback_rows),
                true_fraud=true_fraud,
                false_positive=false_positive,
                needs_review=needs_review,
                precision=precision,
            )
        )
    return rows


def summarize_false_positive_rate_by_reason(
    joined_rows: Iterable[JoinedAlertFeedback],
) -> list[FeedbackSummaryRow]:
    """Compute false positive rates grouped by analyst-readable rule reason."""
    reason_feedback: dict[str, list[AnalystFeedback]] = defaultdict(list)
    for row in joined_rows:
        if row.feedback is None:
            continue
        for reason in row.alert.reasons:
            reason_feedback[reason].append(row.feedback)

    rows: list[FeedbackSummaryRow] = []
    for reason in sorted(reason_feedback):
        feedback_rows = reason_feedback[reason]
        true_fraud = sum(row.label == "true_fraud" for row in feedback_rows)
        false_positive = sum(row.label == "false_positive" for row in feedback_rows)
        needs_review = sum(row.label == "needs_review" for row in feedback_rows)
        decisive = true_fraud + false_positive
        false_positive_rate = false_positive / decisive if decisive else None
        rows.append(
            FeedbackSummaryRow(
                key=reason,
                reviewed_alerts=len(feedback_rows),
                true_fraud=true_fraud,
                false_positive=false_positive,
                needs_review=needs_review,
                false_positive_rate=false_positive_rate,
            )
        )
    return rows


def _label_for_retraining(label: AnalystLabel) -> int | None:
    """Map analyst labels into binary training labels where possible."""
    if label == "true_fraud":
        return 1
    if label == "false_positive":
        return 0
    return None


def build_retraining_export(
    transactions: Iterable[Transaction],
    joined_rows: Iterable[JoinedAlertFeedback],
) -> RetrainingExportResult:
    """Build retraining rows from canonical transaction features and analyst labels."""
    joined_map = {row.alert.transaction_id: row for row in joined_rows if row.feedback is not None}
    states: dict[str, UserProfileState] = {}
    rows: list[RetrainingExportRow] = []
    matched_ids: set[str] = set()

    for transaction in transactions:
        state = states.get(transaction.key, UserProfileState())
        features = compute_features(transaction, state)
        joined = joined_map.get(transaction.transaction_id)
        if joined is not None and joined.feedback is not None:
            matched_ids.add(transaction.transaction_id)
            label = _label_for_retraining(joined.feedback.label)
            rows.append(
                RetrainingExportRow(
                    transaction_id=transaction.transaction_id,
                    user_id=transaction.user_id,
                    card_id=transaction.card_id,
                    event_time=transaction.event_time.isoformat(),
                    reviewer_id=joined.feedback.reviewer_id,
                    analyst_label=joined.feedback.label,
                    reviewed_at=joined.feedback.reviewed_at.isoformat(),
                    risk_score=joined.alert.risk_score,
                    risk_level=joined.alert.risk_level,
                    reasons=joined.alert.reasons,
                    feature_values=build_feature_dict(features, transaction),
                    label=label,
                    label_source="analyst_feedback" if label is not None else "needs_review",
                    review_comment=joined.feedback.comment,
                )
            )
        states[transaction.key] = update_state(transaction, state)

    unmatched_transaction_count = len(joined_map) - len(matched_ids)
    return RetrainingExportResult(
        rows=rows,
        unmatched_feedback_count=0,
        unmatched_transaction_count=unmatched_transaction_count,
    )


def _load_transactions(path: Path) -> list[Transaction]:
    """Load canonical transactions from a JSONL file."""
    payloads = _load_json_objects(path, kind="transaction")
    return [transaction_from_dict(payload) for payload in payloads]


def build_feedback_report(
    alerts: Iterable[Alert],
    feedback_rows: Iterable[AnalystFeedback],
    *,
    transactions: Iterable[Transaction] | None = None,
) -> tuple[FeedbackReport, RetrainingExportResult | None]:
    """Build the full analyst feedback report."""
    joined_rows, unmatched_feedback = join_alerts_with_feedback(alerts, feedback_rows)
    counts = build_review_counts(joined_rows, unmatched_feedback)
    retraining_export: RetrainingExportResult | None = None
    if transactions is not None:
        retraining_export = build_retraining_export(transactions, joined_rows)
        retraining_export = RetrainingExportResult(
            rows=retraining_export.rows,
            unmatched_feedback_count=len(unmatched_feedback),
            unmatched_transaction_count=retraining_export.unmatched_transaction_count,
        )

    report = FeedbackReport(
        counts=counts,
        precision_by_risk_level=summarize_precision_by_risk_level(joined_rows),
        false_positive_rate_by_reason=summarize_false_positive_rate_by_reason(joined_rows),
        unmatched_feedback_transaction_ids=sorted(row.transaction_id for row in unmatched_feedback),
        retraining_export=None if retraining_export is None else retraining_export.to_dict(),
    )
    return report, retraining_export


def save_report(path: Path, report: FeedbackReport) -> None:
    """Write the feedback report JSON file."""
    path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


def save_retraining_export(path: Path, export: RetrainingExportResult) -> None:
    """Write retraining export rows as JSONL."""
    with path.open("w", encoding="utf-8") as handle:
        for row in export.rows:
            handle.write(json.dumps(row.to_dict(), sort_keys=True))
            handle.write("\n")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the analyst feedback report CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        validate_args(args)
        alerts = load_alerts(args.alerts)
        feedback_rows = load_feedback(args.feedback)
        transactions = None if args.transactions is None else _load_transactions(args.transactions)
        report, retraining_export = build_feedback_report(
            alerts,
            feedback_rows,
            transactions=transactions,
        )
        save_report(args.output, report)
        if args.retraining_output is not None:
            if retraining_export is None:
                raise ValueError("retraining export requires transaction input")
            save_retraining_export(args.retraining_output, retraining_export)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        parser.exit(status=2, message=f"{exc}\n")

    print(f"Saved feedback report to {args.output}")
    if args.retraining_output is not None:
        print(f"Saved retraining export to {args.retraining_output}")
    return 0
