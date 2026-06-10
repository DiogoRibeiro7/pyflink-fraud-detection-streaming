"""Typed domain schemas used by the fraud streaming pipeline."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from math import sqrt
from typing import Any, Literal

RiskLevel = Literal["low", "elevated", "medium", "high"]


@dataclass(frozen=True, slots=True)
class Transaction:
    """A card transaction received from the streaming source."""

    transaction_id: str
    user_id: str
    card_id: str
    merchant_id: str
    amount: float
    currency: str
    country: str
    device_id: str
    merchant_category: str
    event_time: datetime
    channel: str
    is_card_present: bool
    latitude: float | None = None
    longitude: float | None = None

    def __post_init__(self) -> None:
        """Validate input values after dataclass construction."""
        if not self.transaction_id:
            raise ValueError("transaction_id cannot be empty")
        if not self.user_id:
            raise ValueError("user_id cannot be empty")
        if not self.card_id:
            raise ValueError("card_id cannot be empty")
        if self.amount < 0:
            raise ValueError("amount cannot be negative")
        if not self.currency or len(self.currency) != 3:
            raise ValueError("currency must be an ISO-4217-like 3-letter code")
        if not self.country or len(self.country) != 2:
            raise ValueError("country must be an ISO-3166-like 2-letter code")
        if self.event_time.tzinfo is None:
            raise ValueError("event_time must be timezone-aware")

    @property
    def event_time_ms(self) -> int:
        """Return event time as milliseconds since Unix epoch."""
        return int(self.event_time.timestamp() * 1_000)

    @property
    def key(self) -> str:
        """Return the state key used by the streaming job."""
        return f"{self.user_id}:{self.card_id}"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible dictionary."""
        return {
            "transaction_id": self.transaction_id,
            "user_id": self.user_id,
            "card_id": self.card_id,
            "merchant_id": self.merchant_id,
            "amount": self.amount,
            "currency": self.currency,
            "country": self.country,
            "device_id": self.device_id,
            "merchant_category": self.merchant_category,
            "event_time": self.event_time.astimezone(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "channel": self.channel,
            "is_card_present": self.is_card_present,
            "latitude": self.latitude,
            "longitude": self.longitude,
        }


@dataclass(frozen=True, slots=True)
class RollingTransaction:
    """Small representation kept in keyed state for rolling features."""

    amount: float
    event_time_ms: int
    country: str
    device_id: str


@dataclass(slots=True)
class UserProfileState:
    """State maintained per user/card key.

    The mean and variance are updated using Welford's online algorithm. The
    rolling transaction list is intentionally small and pruned on every event.
    """

    count: int = 0
    amount_mean: float = 0.0
    amount_m2: float = 0.0
    last_country: str | None = None
    last_device_id: str | None = None
    last_event_time_ms: int | None = None
    rolling_transactions: list[RollingTransaction] = field(default_factory=list)

    @property
    def amount_variance(self) -> float:
        """Return sample variance of historical transaction amount."""
        if self.count < 2:
            return 0.0
        return self.amount_m2 / (self.count - 1)

    @property
    def amount_std(self) -> float:
        """Return sample standard deviation of historical transaction amount."""
        return sqrt(self.amount_variance)

    def copy(self) -> UserProfileState:
        """Return a defensive copy of the state."""
        return UserProfileState(
            count=self.count,
            amount_mean=self.amount_mean,
            amount_m2=self.amount_m2,
            last_country=self.last_country,
            last_device_id=self.last_device_id,
            last_event_time_ms=self.last_event_time_ms,
            rolling_transactions=list(self.rolling_transactions),
        )

    def update_amount_statistics(self, amount: float) -> None:
        """Update online mean and variance statistics with a new amount."""
        self.count += 1
        delta = amount - self.amount_mean
        self.amount_mean += delta / self.count
        delta_2 = amount - self.amount_mean
        self.amount_m2 += delta * delta_2

    def to_dict(self) -> dict[str, Any]:
        """Serialize state to a JSON-compatible dictionary."""
        return {
            "count": self.count,
            "amount_mean": self.amount_mean,
            "amount_m2": self.amount_m2,
            "last_country": self.last_country,
            "last_device_id": self.last_device_id,
            "last_event_time_ms": self.last_event_time_ms,
            "rolling_transactions": [asdict(item) for item in self.rolling_transactions],
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> UserProfileState:
        """Create state from a dictionary."""
        rolling = [
            RollingTransaction(
                amount=float(item["amount"]),
                event_time_ms=int(item["event_time_ms"]),
                country=str(item["country"]),
                device_id=str(item["device_id"]),
            )
            for item in value.get("rolling_transactions", [])
        ]
        return cls(
            count=int(value.get("count", 0)),
            amount_mean=float(value.get("amount_mean", 0.0)),
            amount_m2=float(value.get("amount_m2", 0.0)),
            last_country=value.get("last_country"),
            last_device_id=value.get("last_device_id"),
            last_event_time_ms=value.get("last_event_time_ms"),
            rolling_transactions=rolling,
        )

    def to_json(self) -> str:
        """Serialize state to JSON for Flink ValueState."""
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_json(cls, value: str | None) -> UserProfileState:
        """Deserialize state from JSON, returning empty state for missing values."""
        if value is None or value == "":
            return cls()
        decoded = json.loads(value)
        if not isinstance(decoded, dict):
            raise ValueError("state JSON must decode to an object")
        return cls.from_dict(decoded)


@dataclass(frozen=True, slots=True)
class FraudFeatures:
    """Features computed for a transaction using prior state."""

    transaction_id: str
    user_id: str
    card_id: str
    event_time: datetime
    amount: float
    tx_count_5m: int
    amount_sum_1h: float
    amount_zscore: float
    minutes_since_last_tx: float | None
    country_changed: bool
    device_changed: bool
    card_not_present: bool
    night_transaction: bool
    high_velocity: bool
    high_amount: bool


@dataclass(frozen=True, slots=True)
class Alert:
    """Fraud alert emitted by the streaming job."""

    transaction_id: str
    user_id: str
    card_id: str
    event_time: datetime
    risk_score: int
    risk_level: RiskLevel
    reasons: list[str]
    features: FraudFeatures

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible dictionary."""
        return {
            "transaction_id": self.transaction_id,
            "user_id": self.user_id,
            "card_id": self.card_id,
            "event_time": self.event_time.astimezone(timezone.utc).isoformat(),
            "risk_score": self.risk_score,
            "risk_level": self.risk_level,
            "reasons": self.reasons,
            "features": {
                "amount": self.features.amount,
                "tx_count_5m": self.features.tx_count_5m,
                "amount_sum_1h": self.features.amount_sum_1h,
                "amount_zscore": self.features.amount_zscore,
                "minutes_since_last_tx": self.features.minutes_since_last_tx,
                "country_changed": self.features.country_changed,
                "device_changed": self.features.device_changed,
                "card_not_present": self.features.card_not_present,
                "night_transaction": self.features.night_transaction,
                "high_velocity": self.features.high_velocity,
                "high_amount": self.features.high_amount,
            },
        }
