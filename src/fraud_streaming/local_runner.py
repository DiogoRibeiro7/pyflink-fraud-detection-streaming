"""Local runner for development, tests, and demos without Flink."""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from fraud_streaming.config import DEFAULT_CONFIG, FraudConfig
from fraud_streaming.features import compute_features, update_state
from fraud_streaming.rules import build_alert, score_features
from fraud_streaming.schemas import Alert, Transaction, UserProfileState
from fraud_streaming.serialization import transaction_from_json


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
) -> Iterator[Alert]:
    """Process JSON lines and yield alerts.

    Args:
        lines: Iterable of JSON encoded transactions.
        config: Feature and scoring configuration.
        emit_low_risk: When True, emit every scored transaction. When False,
            only emit elevated, medium, and high-risk alerts.

    Yields:
        Alert objects.
    """
    states: dict[str, UserProfileState] = {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        transaction = transaction_from_json(line)
        alert = process_transaction(transaction, states, config)
        if emit_low_risk or alert.risk_level != "low":
            yield alert
