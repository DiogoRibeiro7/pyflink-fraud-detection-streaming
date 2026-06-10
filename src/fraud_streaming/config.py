"""Runtime configuration for the fraud detection pipeline."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FraudConfig:
    """Configuration for feature engineering and risk scoring.

    Attributes:
        velocity_window_minutes: Window used for short-term transaction velocity.
        amount_window_minutes: Window used for short-term amount accumulation.
        high_amount_threshold: Absolute amount threshold used by transparent rules.
        high_velocity_threshold: Number of transactions in the velocity window that is suspicious.
        high_hourly_amount_threshold: Total amount in the amount window that is suspicious.
        history_min_count_for_zscore: Minimum historical observations before z-score rules apply.
        zscore_threshold: Threshold for unusual amount detection.
    """

    velocity_window_minutes: int = 5
    amount_window_minutes: int = 60
    high_amount_threshold: float = 500.0
    high_velocity_threshold: int = 5
    high_hourly_amount_threshold: float = 2_000.0
    history_min_count_for_zscore: int = 5
    zscore_threshold: float = 3.0

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.velocity_window_minutes <= 0:
            raise ValueError("velocity_window_minutes must be positive")
        if self.amount_window_minutes <= 0:
            raise ValueError("amount_window_minutes must be positive")
        if self.high_amount_threshold <= 0:
            raise ValueError("high_amount_threshold must be positive")
        if self.high_velocity_threshold <= 0:
            raise ValueError("high_velocity_threshold must be positive")
        if self.high_hourly_amount_threshold <= 0:
            raise ValueError("high_hourly_amount_threshold must be positive")
        if self.history_min_count_for_zscore < 2:
            raise ValueError("history_min_count_for_zscore must be at least 2")
        if self.zscore_threshold <= 0:
            raise ValueError("zscore_threshold must be positive")


DEFAULT_CONFIG = FraudConfig()
