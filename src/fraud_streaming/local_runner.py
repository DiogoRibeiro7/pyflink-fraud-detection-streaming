"""Local runner for development, tests, and demos without Flink."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import timedelta
from pathlib import Path
from typing import TextIO

from fraud_streaming.config import DEFAULT_CONFIG, FraudConfig
from fraud_streaming.features import compute_features, update_state
from fraud_streaming.ml.scoring import (
    ModelScorer,
    ScoringConfig,
    combine_scores,
    compute_model_score,
)
from fraud_streaming.observability.metrics import LocalMetricsRegistry
from fraud_streaming.quality import (
    DEFAULT_FUTURE_EVENT_TOLERANCE,
    dead_letter_to_json,
    parse_and_validate_transaction_event,
)
from fraud_streaming.rules import build_alert, score_features
from fraud_streaming.schemas import Alert, Transaction, UserProfileState
from fraud_streaming.sinks import AlertSink, TransactionSink

DEFAULT_SCORING_CONFIG = ScoringConfig()


def process_transaction(
    transaction: Transaction,
    states: dict[str, UserProfileState],
    config: FraudConfig = DEFAULT_CONFIG,
    scoring_config: ScoringConfig = DEFAULT_SCORING_CONFIG,
    model_scorer: ModelScorer | None = None,
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
    rule_score = score_features(features, config)
    model_score = None
    if scoring_config.strategy in {"model", "blend"}:
        if model_scorer is None:
            raise ValueError("model_scorer is required for model or blend strategy")
        model_score = compute_model_score(model_scorer, features, transaction)
    final_score = combine_scores(
        rule_score=rule_score,
        model_score=model_score,
        strategy=scoring_config.strategy,
        rule_weight=scoring_config.rule_weight,
        model_weight=scoring_config.model_weight,
    )
    alert = build_alert(features, final_score)
    states[transaction.key] = update_state(transaction, state, config)
    return alert


def process_json_lines(
    lines: Iterable[str],
    config: FraudConfig = DEFAULT_CONFIG,
    emit_low_risk: bool = False,
    dead_letter_handle: TextIO | None = None,
    future_tolerance: timedelta = DEFAULT_FUTURE_EVENT_TOLERANCE,
    metrics: LocalMetricsRegistry | None = None,
    scoring_config: ScoringConfig = DEFAULT_SCORING_CONFIG,
    model_scorer: ModelScorer | None = None,
    transaction_sink: TransactionSink | None = None,
    alert_sink: AlertSink | None = None,
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
        metrics: Optional local metrics registry for demo observability.
        scoring_config: Rule/model scoring strategy configuration.
        model_scorer: Optional loaded model scorer for model-aware strategies.

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
            if metrics is not None:
                metrics.record_malformed_event()
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
        if transaction_sink is not None:
            transaction_sink.write(transaction)
        alert = process_transaction(
            transaction,
            states,
            config,
            scoring_config=scoring_config,
            model_scorer=model_scorer,
        )
        if metrics is not None:
            metrics.record_processed_transaction(transaction, alert)
        if emit_low_risk or alert.risk_level != "low":
            if metrics is not None:
                metrics.record_emitted_alert(alert)
            if alert_sink is not None:
                alert_sink.write(alert)
            yield alert


def load_model_scorer(scoring_config: ScoringConfig) -> ModelScorer | None:
    """Load the optional model scorer for the requested strategy."""
    if scoring_config.strategy == "rules":
        return None
    if scoring_config.model_artifact_path is None:
        raise ValueError("model_artifact_path is required for model or blend strategy")
    return ModelScorer.from_artifact(Path(scoring_config.model_artifact_path))
