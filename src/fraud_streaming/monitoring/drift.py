"""Drift monitoring for fraud scores and selected features."""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any, cast

from fraud_streaming.config import DEFAULT_CONFIG
from fraud_streaming.features import compute_features, update_state
from fraud_streaming.rules import score_features
from fraud_streaming.schemas import UserProfileState
from fraud_streaming.serialization import transaction_from_dict

DRIFT_FEATURES: tuple[str, ...] = (
    "amount",
    "tx_count_5m",
    "amount_sum_1h",
    "amount_zscore",
    "minutes_since_last_tx",
)
DRIFT_SEGMENTS: tuple[str, ...] = ("country", "merchant_category", "channel", "risk_level")


@dataclass(frozen=True, slots=True)
class DriftRecord:
    """Feature-and-score row used for drift comparisons."""

    risk_score: float
    country: str
    merchant_category: str
    channel: str
    risk_level: str
    feature_values: dict[str, float]


@dataclass(frozen=True, slots=True)
class DriftMetric:
    """One metric result for one score or feature."""

    psi: float
    mean_delta: float
    std_delta: float
    quantile_deltas: dict[str, float]
    ks_statistic: float | None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "psi": self.psi,
            "mean_delta": self.mean_delta,
            "std_delta": self.std_delta,
            "quantile_deltas": self.quantile_deltas,
            "ks_statistic": self.ks_statistic,
        }


@dataclass(frozen=True, slots=True)
class DriftReport:
    """Top-level drift report."""

    reference_count: int
    current_count: int
    overall: dict[str, DriftMetric]
    segments: dict[str, dict[str, dict[str, DriftMetric]]]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "reference_count": self.reference_count,
            "current_count": self.current_count,
            "overall": {name: metric.to_dict() for name, metric in self.overall.items()},
            "segments": {
                segment_name: {
                    segment_value: {
                        metric_name: metric.to_dict() for metric_name, metric in metrics.items()
                    }
                    for segment_value, metrics in segment_values.items()
                }
                for segment_name, segment_values in self.segments.items()
            },
        }


def build_parser() -> argparse.ArgumentParser:
    """Create the drift report CLI parser."""
    parser = argparse.ArgumentParser(description="Generate fraud score and feature drift reports.")
    parser.add_argument("--reference", type=Path, required=True, help="Reference JSONL file.")
    parser.add_argument("--current", type=Path, required=True, help="Current JSONL file.")
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to save the JSON drift report.",
    )
    parser.add_argument(
        "--markdown-output",
        type=Path,
        help="Optional Markdown summary output path.",
    )
    parser.add_argument(
        "--segment-by",
        action="append",
        choices=list(DRIFT_SEGMENTS),
        help="Optional segment dimension to include. Can be repeated.",
    )
    return parser


def validate_args(args: argparse.Namespace) -> None:
    """Validate drift CLI arguments."""
    for name in ("reference", "current"):
        path = getattr(args, name)
        if not path.exists():
            raise ValueError(f"{name} file does not exist: {path}")
        if not path.is_file():
            raise ValueError(f"{name} path is not a file: {path}")


def _load_payloads(path: Path) -> list[dict[str, Any]]:
    """Load JSONL payloads from disk."""
    payloads: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError(f"JSONL row must decode to an object at line {line_number}")
            payloads.append(payload)
    return payloads


def build_drift_records(path: Path) -> list[DriftRecord]:
    """Build feature and score rows from a JSONL stream."""
    states: dict[str, UserProfileState] = {}
    records: list[DriftRecord] = []
    for payload in _load_payloads(path):
        transaction = transaction_from_dict(payload)
        state = states.get(transaction.key, UserProfileState())
        features = compute_features(transaction, state, DEFAULT_CONFIG)
        rule_score = score_features(features, DEFAULT_CONFIG)
        records.append(
            DriftRecord(
                risk_score=float(rule_score.risk_score),
                country=transaction.country,
                merchant_category=transaction.merchant_category,
                channel=transaction.channel,
                risk_level=rule_score.risk_level,
                feature_values={
                    "amount": features.amount,
                    "tx_count_5m": float(features.tx_count_5m),
                    "amount_sum_1h": features.amount_sum_1h,
                    "amount_zscore": features.amount_zscore,
                    "minutes_since_last_tx": (
                        0.0
                        if features.minutes_since_last_tx is None
                        else features.minutes_since_last_tx
                    ),
                },
            )
        )
        states[transaction.key] = update_state(transaction, state, DEFAULT_CONFIG)
    return records


def population_stability_index(
    reference: list[float],
    current: list[float],
    *,
    bins: int = 10,
) -> float:
    """Compute PSI using equal-width bins on the combined range."""
    if not reference or not current:
        return 0.0
    if bins <= 0:
        raise ValueError("bins must be positive")

    lower = min(min(reference), min(current))
    upper = max(max(reference), max(current))
    if math.isclose(lower, upper):
        return 0.0

    width = (upper - lower) / bins
    edges = [lower + width * index for index in range(bins)]
    edges.append(upper)

    def _counts(values: list[float]) -> list[int]:
        counts = [0] * bins
        for value in values:
            if value == upper:
                counts[-1] += 1
                continue
            index = min(int((value - lower) / width), bins - 1)
            counts[index] += 1
        return counts

    reference_counts = _counts(reference)
    current_counts = _counts(current)
    psi = 0.0
    for reference_count, current_count in zip(reference_counts, current_counts, strict=True):
        ref_ratio = max(reference_count / len(reference), 1e-6)
        cur_ratio = max(current_count / len(current), 1e-6)
        psi += (cur_ratio - ref_ratio) * math.log(cur_ratio / ref_ratio)
    return psi


def _std(values: list[float]) -> float:
    """Compute sample standard deviation."""
    if len(values) < 2:
        return 0.0
    average = mean(values)
    variance = sum((value - average) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def _quantile(values: list[float], q: float) -> float:
    """Compute a simple linear-interpolated quantile."""
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower_index = int(math.floor(position))
    upper_index = int(math.ceil(position))
    if lower_index == upper_index:
        return ordered[lower_index]
    lower = ordered[lower_index]
    upper = ordered[upper_index]
    return lower + (upper - lower) * (position - lower_index)


def _optional_ks_statistic(reference: list[float], current: list[float]) -> float | None:
    """Compute KS statistic when scipy is available."""
    try:
        from scipy.stats import ks_2samp
    except ImportError:
        return None
    result = ks_2samp(reference, current)
    return float(result.statistic)


def compute_drift_metric(reference: list[float], current: list[float]) -> DriftMetric:
    """Compute drift metrics for one numeric series."""
    if not reference or not current:
        raise ValueError("reference and current series must be non-empty")

    return DriftMetric(
        psi=population_stability_index(reference, current),
        mean_delta=mean(current) - mean(reference),
        std_delta=_std(current) - _std(reference),
        quantile_deltas={
            "p25": _quantile(current, 0.25) - _quantile(reference, 0.25),
            "p50": _quantile(current, 0.50) - _quantile(reference, 0.50),
            "p75": _quantile(current, 0.75) - _quantile(reference, 0.75),
        },
        ks_statistic=_optional_ks_statistic(reference, current),
    )


def _metric_series(records: list[DriftRecord], metric_name: str) -> list[float]:
    """Extract one metric series from drift records."""
    if metric_name == "risk_score":
        return [record.risk_score for record in records]
    return [record.feature_values[metric_name] for record in records]


def compute_overall_drift(
    reference_records: list[DriftRecord],
    current_records: list[DriftRecord],
) -> dict[str, DriftMetric]:
    """Compute overall drift metrics for score and selected features."""
    metrics: dict[str, DriftMetric] = {}
    for metric_name in ("risk_score", *DRIFT_FEATURES):
        metrics[metric_name] = compute_drift_metric(
            _metric_series(reference_records, metric_name),
            _metric_series(current_records, metric_name),
        )
    return metrics


def compute_segmented_drift(
    reference_records: list[DriftRecord],
    current_records: list[DriftRecord],
    segment_names: list[str],
) -> dict[str, dict[str, dict[str, DriftMetric]]]:
    """Compute segmented drift metrics for requested segment dimensions."""
    segmented: dict[str, dict[str, dict[str, DriftMetric]]] = {}
    for segment_name in segment_names:
        segment_report: dict[str, dict[str, DriftMetric]] = {}
        reference_values = {getattr(record, segment_name) for record in reference_records}
        current_values = {getattr(record, segment_name) for record in current_records}
        for segment_value in sorted(reference_values | current_values):
            ref_subset = [
                record
                for record in reference_records
                if getattr(record, segment_name) == segment_value
            ]
            cur_subset = [
                record
                for record in current_records
                if getattr(record, segment_name) == segment_value
            ]
            if len(ref_subset) < 2 or len(cur_subset) < 2:
                continue
            segment_report[segment_value] = compute_overall_drift(ref_subset, cur_subset)
        segmented[segment_name] = segment_report
    return segmented


def build_drift_report(
    reference_records: list[DriftRecord],
    current_records: list[DriftRecord],
    segment_names: list[str],
) -> DriftReport:
    """Build a full drift report."""
    if not reference_records:
        raise ValueError("reference dataset is empty")
    if not current_records:
        raise ValueError("current dataset is empty")
    return DriftReport(
        reference_count=len(reference_records),
        current_count=len(current_records),
        overall=compute_overall_drift(reference_records, current_records),
        segments=compute_segmented_drift(reference_records, current_records, segment_names),
    )


def save_json_report(report: DriftReport, output_path: Path) -> None:
    """Write the JSON drift report to disk."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


def render_markdown_report(report: DriftReport) -> str:
    """Render a concise Markdown drift summary."""
    lines = [
        "# Drift Report",
        "",
        f"- Reference rows: {report.reference_count}",
        f"- Current rows: {report.current_count}",
        "",
        "## Overall",
        "",
        "| Metric | PSI | Mean Delta | Std Delta | KS |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for metric_name, metric in report.overall.items():
        ks = "n/a" if metric.ks_statistic is None else f"{metric.ks_statistic:.4f}"
        lines.append(
            f"| {metric_name} | {metric.psi:.4f} | {metric.mean_delta:.4f} | "
            f"{metric.std_delta:.4f} | {ks} |"
        )
    return "\n".join(lines) + "\n"


def save_markdown_report(report: DriftReport, output_path: Path) -> None:
    """Write the Markdown drift summary to disk."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_markdown_report(report), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    """Run the drift report CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        validate_args(args)
        segment_names = cast(list[str], args.segment_by or [])
        reference_records = build_drift_records(args.reference)
        current_records = build_drift_records(args.current)
        report = build_drift_report(reference_records, current_records, segment_names)
        save_json_report(report, args.output)
        if args.markdown_output is not None:
            save_markdown_report(report, args.markdown_output)
    except ValueError as exc:
        parser.exit(status=2, message=f"{exc}\n")

    print(json.dumps(report.to_dict(), sort_keys=True))
    return 0
