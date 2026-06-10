"""Command-line interface for local fraud detection demos."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from fraud_streaming.local_runner import load_model_scorer, process_json_lines
from fraud_streaming.ml.scoring import ScoringConfig
from fraud_streaming.observability.metrics import LocalMetricsRegistry
from fraud_streaming.sinks import (
    AlertSink,
    IcebergAlertSink,
    IcebergSinkConfig,
    IcebergTransactionSink,
    JsonlAlertSink,
    JsonlTransactionSink,
    NullTransactionSink,
    ParquetAlertSink,
    ParquetTransactionSink,
    StdoutAlertSink,
    TransactionSink,
    validate_local_sink_args,
)


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
        "--alert-sink",
        choices=["stdout", "jsonl", "parquet", "iceberg"],
        default="stdout",
        help="Where to write emitted alerts.",
    )
    parser.add_argument(
        "--alert-output",
        type=Path,
        help="Output path when --alert-sink is jsonl or parquet.",
    )
    parser.add_argument(
        "--transaction-sink",
        choices=["none", "jsonl", "parquet", "iceberg"],
        default="none",
        help="Optional sink for validated transactions.",
    )
    parser.add_argument(
        "--transaction-output",
        type=Path,
        help="Output path when --transaction-sink is jsonl or parquet.",
    )
    parser.add_argument(
        "--iceberg-catalog-uri",
        help="Catalog URI when using iceberg sinks.",
    )
    parser.add_argument(
        "--iceberg-warehouse",
        help="Warehouse path or URI when using iceberg sinks.",
    )
    parser.add_argument(
        "--iceberg-alert-table",
        help="Target Iceberg table name when --alert-sink=iceberg.",
    )
    parser.add_argument(
        "--iceberg-transaction-table",
        help="Target Iceberg table name when --transaction-sink=iceberg.",
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
        sink_config = validate_local_sink_args(args)
        scoring_config = ScoringConfig(
            strategy=args.scoring_strategy,
            model_artifact_path=args.model_artifact,
            rule_weight=args.rule_weight,
            model_weight=args.model_weight,
        )
        model_scorer = load_model_scorer(scoring_config)
    except ValueError as exc:
        parser.error(str(exc))

    def _iceberg_config(table_name: str | None) -> IcebergSinkConfig:
        if sink_config.iceberg_catalog_uri is None or sink_config.iceberg_warehouse is None:
            raise ValueError("iceberg sink configuration is incomplete")
        if table_name is None:
            raise ValueError("iceberg sink table name is required")
        return IcebergSinkConfig(
            catalog_uri=sink_config.iceberg_catalog_uri,
            warehouse=sink_config.iceberg_warehouse,
            table_name=table_name,
        )

    alert_sink: AlertSink
    try:
        if sink_config.alert_sink == "stdout":
            alert_sink = StdoutAlertSink()
        elif sink_config.alert_sink == "jsonl":
            if sink_config.alert_output is None:
                parser.error("--alert-output is required when --alert-sink=jsonl")
            alert_sink = JsonlAlertSink(sink_config.alert_output)
        elif sink_config.alert_sink == "parquet":
            if sink_config.alert_output is None:
                parser.error("--alert-output is required when --alert-sink=parquet")
            alert_sink = ParquetAlertSink(sink_config.alert_output)
        else:
            alert_sink = IcebergAlertSink(_iceberg_config(sink_config.iceberg_alert_table))
    except RuntimeError as exc:
        parser.error(str(exc))

    transaction_sink: TransactionSink
    try:
        if sink_config.transaction_sink == "none":
            transaction_sink = NullTransactionSink()
        elif sink_config.transaction_sink == "jsonl":
            if sink_config.transaction_output is None:
                parser.error("--transaction-output is required when --transaction-sink=jsonl")
            transaction_sink = JsonlTransactionSink(sink_config.transaction_output)
        elif sink_config.transaction_sink == "parquet":
            if sink_config.transaction_output is None:
                parser.error("--transaction-output is required when --transaction-sink=parquet")
            transaction_sink = ParquetTransactionSink(sink_config.transaction_output)
        else:
            transaction_sink = IcebergTransactionSink(
                _iceberg_config(sink_config.iceberg_transaction_table)
            )
    except RuntimeError as exc:
        parser.error(str(exc))

    try:
        with input_path.open("r", encoding="utf-8") as handle:
            if dead_letter_output is None:
                for _alert in process_json_lines(
                    handle,
                    emit_low_risk=args.show_all,
                    metrics=metrics,
                    scoring_config=scoring_config,
                    model_scorer=model_scorer,
                    transaction_sink=transaction_sink,
                    alert_sink=alert_sink,
                ):
                    pass
            else:
                dead_letter_output.parent.mkdir(parents=True, exist_ok=True)
                with dead_letter_output.open("w", encoding="utf-8") as dead_letter_handle:
                    for _alert in process_json_lines(
                        handle,
                        emit_low_risk=args.show_all,
                        dead_letter_handle=dead_letter_handle,
                        metrics=metrics,
                        scoring_config=scoring_config,
                        model_scorer=model_scorer,
                        transaction_sink=transaction_sink,
                        alert_sink=alert_sink,
                    ):
                        pass
    finally:
        alert_sink.close()
        transaction_sink.close()

    if metrics_output is not None and metrics is not None:
        metrics_output.parent.mkdir(parents=True, exist_ok=True)
        metrics_output.write_text(metrics.to_prometheus_text(), encoding="utf-8")

    return 0


if __name__ == "__main__":
    sys.exit(main())
