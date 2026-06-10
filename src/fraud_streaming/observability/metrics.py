"""Prometheus-friendly local metrics for fraud processing demos."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

from fraud_streaming.schemas import Alert, Transaction

DEFAULT_METRIC_HELP: Final[dict[str, str]] = {
    "fraud_transactions_processed_total": "Total number of successfully processed transactions.",
    "fraud_alerts_emitted_total": "Total number of emitted alerts after local filtering.",
    "fraud_high_risk_alerts_total": "Total number of emitted high-risk alerts.",
    "fraud_malformed_events_total": "Total number of malformed or rejected input events.",
    "fraud_average_risk_score": "Average risk score of successfully processed transactions.",
    "fraud_events_by_country_total": "Processed transactions by country.",
    "fraud_events_by_channel_total": "Processed transactions by channel.",
    "fraud_events_by_risk_level_total": "Processed transactions by risk level.",
}


@dataclass(slots=True)
class CounterMetric:
    """Simple counter metric with optional labels."""

    values: dict[tuple[str, ...], float] = field(default_factory=dict)

    def increment(self, amount: float = 1.0, labels: tuple[str, ...] = ()) -> None:
        """Increment the counter by a non-negative amount."""
        if amount < 0:
            raise ValueError("counter increments must be non-negative")
        self.values[labels] = self.values.get(labels, 0.0) + amount


@dataclass(slots=True)
class GaugeMetric:
    """Simple gauge metric with optional labels."""

    values: dict[tuple[str, ...], float] = field(default_factory=dict)

    def set(self, value: float, labels: tuple[str, ...] = ()) -> None:
        """Set the gauge value for a label combination."""
        self.values[labels] = value


@dataclass(slots=True)
class LocalMetricsRegistry:
    """Registry for local demo metrics with Prometheus text export."""

    counters: dict[str, CounterMetric] = field(default_factory=dict)
    gauges: dict[str, GaugeMetric] = field(default_factory=dict)
    _risk_score_total: float = 0.0

    def __post_init__(self) -> None:
        self.counters = {
            "fraud_transactions_processed_total": CounterMetric(),
            "fraud_alerts_emitted_total": CounterMetric(),
            "fraud_high_risk_alerts_total": CounterMetric(),
            "fraud_malformed_events_total": CounterMetric(),
            "fraud_events_by_country_total": CounterMetric(),
            "fraud_events_by_channel_total": CounterMetric(),
            "fraud_events_by_risk_level_total": CounterMetric(),
        }
        self.gauges = {"fraud_average_risk_score": GaugeMetric()}

    def record_processed_transaction(self, transaction: Transaction, alert: Alert) -> None:
        """Update metrics for a successfully processed transaction."""
        self.counters["fraud_transactions_processed_total"].increment()
        self.counters["fraud_events_by_country_total"].increment(labels=(transaction.country,))
        self.counters["fraud_events_by_channel_total"].increment(labels=(transaction.channel,))
        self.counters["fraud_events_by_risk_level_total"].increment(labels=(alert.risk_level,))

        self._risk_score_total += alert.risk_score
        total_processed = self.counters["fraud_transactions_processed_total"].values.get((), 0.0)
        average = self._risk_score_total / total_processed if total_processed > 0 else 0.0
        self.gauges["fraud_average_risk_score"].set(average)

    def record_emitted_alert(self, alert: Alert) -> None:
        """Update metrics for an emitted alert."""
        self.counters["fraud_alerts_emitted_total"].increment()
        if alert.risk_level == "high":
            self.counters["fraud_high_risk_alerts_total"].increment()

    def record_malformed_event(self) -> None:
        """Update metrics for a malformed event."""
        self.counters["fraud_malformed_events_total"].increment()

    def to_prometheus_text(self) -> str:
        """Export deterministic Prometheus text format."""
        lines: list[str] = []
        for metric_name in sorted(self.counters):
            lines.extend(
                self._render_metric(metric_name, "counter", self.counters[metric_name].values)
            )
        for metric_name in sorted(self.gauges):
            lines.extend(self._render_metric(metric_name, "gauge", self.gauges[metric_name].values))
        return "\n".join(lines) + "\n"

    def _render_metric(
        self,
        metric_name: str,
        metric_type: str,
        values: dict[tuple[str, ...], float],
    ) -> list[str]:
        """Render one metric family in Prometheus text format."""
        lines = [
            f"# HELP {metric_name} {DEFAULT_METRIC_HELP[metric_name]}",
            f"# TYPE {metric_name} {metric_type}",
        ]
        label_names = _label_names_for_metric(metric_name)
        if not values:
            lines.append(f"{metric_name} 0")
            return lines

        for labels in sorted(values):
            value = values[labels]
            if label_names:
                rendered_labels = ",".join(
                    f'{name}="{label}"' for name, label in zip(label_names, labels, strict=True)
                )
                lines.append(f"{metric_name}{{{rendered_labels}}} {value:g}")
            else:
                lines.append(f"{metric_name} {value:g}")
        return lines


def _label_names_for_metric(metric_name: str) -> tuple[str, ...]:
    """Return the ordered label names for one metric family."""
    if metric_name == "fraud_events_by_country_total":
        return ("country",)
    if metric_name == "fraud_events_by_channel_total":
        return ("channel",)
    if metric_name == "fraud_events_by_risk_level_total":
        return ("risk_level",)
    return ()
