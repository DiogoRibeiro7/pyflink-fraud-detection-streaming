"""Command-line interface for local fraud detection demos."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from fraud_streaming.local_runner import load_model_scorer, process_json_lines
from fraud_streaming.ml.scoring import ScoringConfig
from fraud_streaming.observability.metrics import LocalMetricsRegistry
from fraud_streaming.serialization import alert_to_json


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser."""
    parser = argparse.ArgumentParser(description="Run local fraud detection over a JSONL file.")
    parser.add_argument("input", type=Path, help="Path to a JSONL transaction file.")
    parser.add_argument(
        "--show-all",
        action="store_true",
        help="Show low-risk transactions as well as elevated alerts.",
    )
    parser.add_argument(
        "--dead-letter-output",
        type=Path,
        help="Optional JSONL path for malformed events that should not stop the run.",
    )
    parser.add_argument(
        "--metrics-output",
        type=Path,
        help="Optional output path for Prometheus text metrics.",
    )
    parser.add_argument(
        "--scoring-strategy",
        choices=["rules", "model", "blend"],
        default="rules",
        help="How to combine rule-based and model-based fraud scores.",
    )
    parser.add_argument(
        "--model-artifact",
        type=Path,
        help="Optional model artifact path for model or blend scoring.",
    )
    parser.add_argument(
        "--rule-weight",
        type=float,
        default=0.5,
        help="Rule score weight for blended scoring.",
    )
    parser.add_argument(
        "--model-weight",
        type=float,
        default=0.5,
        help="Model score weight for blended scoring.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the local fraud detection CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    input_path: Path = args.input
    if not input_path.exists():
        parser.error(f"input file does not exist: {input_path}")
    if not input_path.is_file():
        parser.error(f"input path is not a file: {input_path}")

    dead_letter_output: Path | None = args.dead_letter_output
    metrics_output: Path | None = args.metrics_output
    if (
        dead_letter_output is not None
        and dead_letter_output.exists()
        and dead_letter_output.is_dir()
    ):
        parser.error(f"dead-letter output path is a directory: {dead_letter_output}")
    if metrics_output is not None and metrics_output.exists() and metrics_output.is_dir():
        parser.error(f"metrics output path is a directory: {metrics_output}")

    metrics = LocalMetricsRegistry() if metrics_output is not None else None
    try:
        scoring_config = ScoringConfig(
            strategy=args.scoring_strategy,
            model_artifact_path=args.model_artifact,
            rule_weight=args.rule_weight,
            model_weight=args.model_weight,
        )
        model_scorer = load_model_scorer(scoring_config)
    except ValueError as exc:
        parser.error(str(exc))

    with input_path.open("r", encoding="utf-8") as handle:
        if dead_letter_output is None:
            for alert in process_json_lines(
                handle,
                emit_low_risk=args.show_all,
                metrics=metrics,
                scoring_config=scoring_config,
                model_scorer=model_scorer,
            ):
                print(alert_to_json(alert))
        else:
            dead_letter_output.parent.mkdir(parents=True, exist_ok=True)
            with dead_letter_output.open("w", encoding="utf-8") as dead_letter_handle:
                for alert in process_json_lines(
                    handle,
                    emit_low_risk=args.show_all,
                    dead_letter_handle=dead_letter_handle,
                    metrics=metrics,
                    scoring_config=scoring_config,
                    model_scorer=model_scorer,
                ):
                    print(alert_to_json(alert))

    if metrics_output is not None and metrics is not None:
        metrics_output.parent.mkdir(parents=True, exist_ok=True)
        metrics_output.write_text(metrics.to_prometheus_text(), encoding="utf-8")

    return 0


if __name__ == "__main__":
    sys.exit(main())
