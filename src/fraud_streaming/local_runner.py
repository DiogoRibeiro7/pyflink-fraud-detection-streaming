"""Local runner for development, tests, and demos without Flink."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import timedelta
from typing import TextIO

from fraud_streaming.config import DEFAULT_CONFIG, FraudConfig
from fraud_streaming.features import compute_features, update_state
from fraud_streaming.quality import (
    DEFAULT_FUTURE_EVENT_TOLERANCE,
    dead_letter_to_json,
    parse_and_validate_transaction_event,
)
from fraud_streaming.rules import build_alert, score_features
from fraud_streaming.schemas import Alert, Transaction, UserProfileState


def process_transaction(
    transaction: Transaction,
    states: dict[str, UserProfileState],
    config: FraudConfig = DEFAULT_CONFIG,
) -> Alert:
    """Process one transaction and update the mutable state dictionary.

    Args:
        transaction: Transaction to process.
        states: Mutable mapping from transaction key to user/card state.
        config: Feature and scoring configuration.

    Returns:
        Alert object. Low-risk transactions are represented as low-risk alerts in
        local mode to make debugging easier. Production sinks can filter them.
    """
    if not isinstance(transaction, Transaction):
        raise TypeError("transaction must be a Transaction")
    if not isinstance(states, dict):
        raise TypeError("states must be a dictionary")

    state = states.get(transaction.key, UserProfileState())
    features = compute_features(transaction, state, config)
    score = score_features(features, config)
    alert = build_alert(features, score)
    states[transaction.key] = update_state(transaction, state, config)
    return alert


def process_json_lines(
    lines: Iterable[str],
    config: FraudConfig = DEFAULT_CONFIG,
    emit_low_risk: bool = False,
    dead_letter_handle: TextIO | None = None,
    future_tolerance: timedelta = DEFAULT_FUTURE_EVENT_TOLERANCE,
) -> Iterator[Alert]:
    """Process JSON lines and yield alerts.

    Args:
        lines: Iterable of JSON encoded transactions.
        config: Feature and scoring configuration.
        emit_low_risk: When True, emit every scored transaction. When False,
            only emit elevated, medium, and high-risk alerts.
        dead_letter_handle: Optional writable handle that receives malformed
            events as JSONL dead-letter records.
        future_tolerance: Allowed future skew for event timestamps.

    Yields:
        Alert objects.
    """
    states: dict[str, UserProfileState] = {}
    seen_transaction_ids: set[str] = set()
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        validated = parse_and_validate_transaction_event(
            line,
            seen_transaction_ids=seen_transaction_ids,
            future_tolerance=future_tolerance,
        )
        if validated.dead_letter is not None:
            if dead_letter_handle is not None:
                dead_letter_handle.write(dead_letter_to_json(validated.dead_letter) + "\n")
            else:
                if validated.dead_letter.parse_error is not None:
                    raise ValueError(validated.dead_letter.parse_error)
                first_failure = validated.dead_letter.quality_failures[0]
                raise ValueError(first_failure.message)
            continue
        if validated.transaction is None:
            raise ValueError("validated transaction result cannot be empty")
        transaction = validated.transaction
        alert = process_transaction(transaction, states, config)
        if emit_low_risk or alert.risk_level != "low":
            yield alert
