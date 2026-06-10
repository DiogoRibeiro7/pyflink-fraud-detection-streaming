"""Stateful fraud feature engineering."""

from __future__ import annotations

from datetime import time

from fraud_streaming.config import DEFAULT_CONFIG, FraudConfig
from fraud_streaming.schemas import FraudFeatures, RollingTransaction, Transaction, UserProfileState

MILLISECONDS_PER_MINUTE = 60_000


def _is_night_transaction(transaction: Transaction) -> bool:
    """Return whether a transaction occurred during a simple night-time window."""
    local_time = transaction.event_time.time()
    return local_time >= time(hour=22) or local_time < time(hour=6)


def _prune_rolling_state(
    rolling_transactions: list[RollingTransaction],
    event_time_ms: int,
    retention_minutes: int,
) -> list[RollingTransaction]:
    """Keep only rolling events inside the maximum feature retention window."""
    cutoff = event_time_ms - retention_minutes * MILLISECONDS_PER_MINUTE
    return [item for item in rolling_transactions if item.event_time_ms >= cutoff]


def _count_transactions_since(
    rolling_transactions: list[RollingTransaction],
    event_time_ms: int,
    window_minutes: int,
) -> int:
    """Count prior transactions inside a rolling window."""
    cutoff = event_time_ms - window_minutes * MILLISECONDS_PER_MINUTE
    return sum(1 for item in rolling_transactions if item.event_time_ms >= cutoff)


def _sum_amount_since(
    rolling_transactions: list[RollingTransaction],
    event_time_ms: int,
    window_minutes: int,
) -> float:
    """Sum prior transaction amounts inside a rolling window."""
    cutoff = event_time_ms - window_minutes * MILLISECONDS_PER_MINUTE
    return sum(item.amount for item in rolling_transactions if item.event_time_ms >= cutoff)


def compute_features(
    transaction: Transaction,
    state: UserProfileState,
    config: FraudConfig = DEFAULT_CONFIG,
) -> FraudFeatures:
    """Compute fraud features for a transaction using prior state.

    Args:
        transaction: Current transaction to score.
        state: Historical state for the transaction key. The state is not mutated.
        config: Feature and scoring thresholds.

    Returns:
        Fraud features including velocity, amount, and context-change signals.
    """
    if not isinstance(transaction, Transaction):
        raise TypeError("transaction must be a Transaction")
    if not isinstance(state, UserProfileState):
        raise TypeError("state must be a UserProfileState")

    max_retention = max(config.velocity_window_minutes, config.amount_window_minutes)
    rolling = _prune_rolling_state(
        state.rolling_transactions,
        transaction.event_time_ms,
        max_retention,
    )

    prior_count_5m = _count_transactions_since(
        rolling,
        transaction.event_time_ms,
        config.velocity_window_minutes,
    )
    prior_amount_sum_1h = _sum_amount_since(
        rolling,
        transaction.event_time_ms,
        config.amount_window_minutes,
    )

    tx_count_5m = prior_count_5m + 1
    amount_sum_1h = prior_amount_sum_1h + transaction.amount

    if state.count >= config.history_min_count_for_zscore and state.amount_std > 1e-9:
        amount_zscore = abs((transaction.amount - state.amount_mean) / state.amount_std)
    else:
        amount_zscore = 0.0

    minutes_since_last_tx: float | None
    if state.last_event_time_ms is None:
        minutes_since_last_tx = None
    else:
        delta_ms = transaction.event_time_ms - state.last_event_time_ms
        minutes_since_last_tx = max(delta_ms / MILLISECONDS_PER_MINUTE, 0.0)

    country_changed = state.last_country is not None and state.last_country != transaction.country
    device_changed = (
        state.last_device_id is not None and state.last_device_id != transaction.device_id
    )
    card_not_present = not transaction.is_card_present
    night_transaction = _is_night_transaction(transaction)
    high_velocity = tx_count_5m >= config.high_velocity_threshold
    high_amount = transaction.amount >= config.high_amount_threshold

    return FraudFeatures(
        transaction_id=transaction.transaction_id,
        user_id=transaction.user_id,
        card_id=transaction.card_id,
        event_time=transaction.event_time,
        amount=transaction.amount,
        tx_count_5m=tx_count_5m,
        amount_sum_1h=amount_sum_1h,
        amount_zscore=amount_zscore,
        minutes_since_last_tx=minutes_since_last_tx,
        country_changed=country_changed,
        device_changed=device_changed,
        card_not_present=card_not_present,
        night_transaction=night_transaction,
        high_velocity=high_velocity,
        high_amount=high_amount,
    )


def update_state(
    transaction: Transaction,
    state: UserProfileState,
    config: FraudConfig = DEFAULT_CONFIG,
) -> UserProfileState:
    """Return an updated copy of state after processing a transaction."""
    if not isinstance(transaction, Transaction):
        raise TypeError("transaction must be a Transaction")
    if not isinstance(state, UserProfileState):
        raise TypeError("state must be a UserProfileState")

    updated = state.copy()
    max_retention = max(config.velocity_window_minutes, config.amount_window_minutes)
    updated.rolling_transactions = _prune_rolling_state(
        updated.rolling_transactions,
        transaction.event_time_ms,
        max_retention,
    )
    updated.rolling_transactions.append(
        RollingTransaction(
            amount=transaction.amount,
            event_time_ms=transaction.event_time_ms,
            country=transaction.country,
            device_id=transaction.device_id,
        )
    )
    updated.update_amount_statistics(transaction.amount)
    updated.last_country = transaction.country
    updated.last_device_id = transaction.device_id
    updated.last_event_time_ms = transaction.event_time_ms
    return updated
