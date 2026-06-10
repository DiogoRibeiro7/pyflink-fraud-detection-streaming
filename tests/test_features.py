from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fraud_streaming.features import compute_features, update_state
from fraud_streaming.schemas import Transaction, UserProfileState


def make_transaction(
    transaction_id: str,
    amount: float,
    event_time: datetime,
    country: str = "PT",
    device_id: str = "device-1",
    card_present: bool = True,
) -> Transaction:
    return Transaction(
        transaction_id=transaction_id,
        user_id="user-1",
        card_id="card-1",
        merchant_id="merchant-1",
        amount=amount,
        currency="EUR",
        country=country,
        device_id=device_id,
        merchant_category="grocery",
        event_time=event_time,
        channel="pos" if card_present else "online",
        is_card_present=card_present,
    )


def test_compute_features_detects_velocity_country_and_device_change() -> None:
    base = datetime(2026, 6, 10, 12, 0, tzinfo=UTC)
    state = UserProfileState()

    for index in range(4):
        tx = make_transaction(f"tx-{index}", 20.0, base + timedelta(minutes=index))
        state = update_state(tx, state)

    current = make_transaction(
        "tx-current",
        25.0,
        base + timedelta(minutes=4),
        country="US",
        device_id="device-2",
    )

    features = compute_features(current, state)

    assert features.tx_count_5m == 5
    assert features.high_velocity is True
    assert features.country_changed is True
    assert features.device_changed is True
    assert features.minutes_since_last_tx == 1.0


def test_update_state_updates_welford_statistics() -> None:
    base = datetime(2026, 6, 10, 12, 0, tzinfo=UTC)
    state = UserProfileState()

    state = update_state(make_transaction("tx-1", 10.0, base), state)
    state = update_state(make_transaction("tx-2", 20.0, base + timedelta(minutes=1)), state)
    state = update_state(make_transaction("tx-3", 30.0, base + timedelta(minutes=2)), state)

    assert state.count == 3
    assert state.amount_mean == 20.0
    assert round(state.amount_variance, 4) == 100.0
