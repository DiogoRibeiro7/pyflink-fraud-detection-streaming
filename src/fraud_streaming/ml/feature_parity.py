"""Offline versus streaming-style feature parity checks."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fraud_streaming.ml.training import (
    CANONICAL_FEATURE_SCHEMA,
    TrainingExample,
    build_training_dataset,
    iter_training_payloads,
)


@dataclass(frozen=True, slots=True)
class ParityCheckResult:
    """One parity check outcome."""

    check_name: str
    passed: bool
    message: str

    def to_dict(self) -> dict[str, str | bool]:
        """Return a JSON-compatible representation."""
        return {"check_name": self.check_name, "passed": self.passed, "message": self.message}


@dataclass(frozen=True, slots=True)
class ParityReport:
    """Complete parity report for two feature datasets."""

    passed: bool
    checks: list[ParityCheckResult]
    compared_rows: int

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible report."""
        return {
            "passed": self.passed,
            "compared_rows": self.compared_rows,
            "checks": [check.to_dict() for check in self.checks],
        }


def build_parser() -> argparse.ArgumentParser:
    """Create the feature parity CLI parser."""
    parser = argparse.ArgumentParser(
        description="Compare offline and streaming-style feature datasets for parity."
    )
    parser.add_argument("--reference", type=Path, required=True, help="Reference JSONL input file.")
    parser.add_argument("--current", type=Path, required=True, help="Current JSONL input file.")
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to save the JSON parity report.",
    )
    parser.add_argument(
        "--deterministic",
        action="store_true",
        help="Require exact row-by-row feature equality in addition to schema checks.",
    )
    return parser


def validate_args(args: argparse.Namespace) -> None:
    """Validate CLI arguments for parity checking."""
    for name in ("reference", "current"):
        path = getattr(args, name)
        if not path.exists():
            raise ValueError(f"{name} file does not exist: {path}")
        if not path.is_file():
            raise ValueError(f"{name} path is not a file: {path}")


def build_feature_rows_from_jsonl(input_path: Path) -> list[TrainingExample]:
    """Build ordered feature rows from a JSONL file using the canonical training path."""
    payloads = iter_training_payloads(
        input_path=input_path,
        input_format="jsonl",
        dataset_mapping_path=None,
        users=0,
        transactions=0,
        seed=0,
    )
    dataset = build_training_dataset(payloads)
    return dataset.examples


def _infer_feature_kind(value: float | str) -> str:
    """Infer a coarse feature kind for parity checks."""
    return "string" if isinstance(value, str) else "numeric"


def compare_feature_datasets(
    reference_rows: list[TrainingExample],
    current_rows: list[TrainingExample],
    *,
    deterministic: bool = False,
) -> ParityReport:
    """Compare offline and current feature datasets for parity."""
    checks: list[ParityCheckResult] = []
    passed = True

    reference_schema = tuple(reference_rows[0].feature_values.keys()) if reference_rows else ()
    current_schema = tuple(current_rows[0].feature_values.keys()) if current_rows else ()

    schema_present = reference_schema == current_schema
    checks.append(
        ParityCheckResult(
            check_name="column_presence",
            passed=schema_present,
            message=(
                "feature columns match"
                if schema_present
                else (
                    "feature columns differ: "
                    f"reference={reference_schema}, current={current_schema}"
                )
            ),
        )
    )
    passed = passed and schema_present

    schema_order = (
        reference_schema == CANONICAL_FEATURE_SCHEMA and current_schema == CANONICAL_FEATURE_SCHEMA
    )
    checks.append(
        ParityCheckResult(
            check_name="column_order",
            passed=schema_order,
            message=(
                "feature column order matches the canonical schema"
                if schema_order
                else "feature column order does not match the canonical schema"
            ),
        )
    )
    passed = passed and schema_order

    shared_schema = tuple(
        name
        for name in CANONICAL_FEATURE_SCHEMA
        if name in reference_schema and name in current_schema
    )
    kind_mismatches: list[str] = []
    for feature_name in shared_schema:
        reference_kind = {
            _infer_feature_kind(row.feature_values[feature_name]) for row in reference_rows
        }
        current_kind = {
            _infer_feature_kind(row.feature_values[feature_name]) for row in current_rows
        }
        if reference_kind != current_kind:
            kind_mismatches.append(
                f"{feature_name}: "
                f"reference={sorted(reference_kind)}, current={sorted(current_kind)}"
            )
    dtype_ok = not kind_mismatches
    checks.append(
        ParityCheckResult(
            check_name="dtype_compatibility",
            passed=dtype_ok,
            message="feature kinds are compatible" if dtype_ok else "; ".join(kind_mismatches),
        )
    )
    passed = passed and dtype_ok

    null_messages: list[str] = []
    for feature_name in shared_schema:
        reference_null_rate = sum(
            1 for row in reference_rows if row.feature_values.get(feature_name) in {None, ""}
        ) / max(len(reference_rows), 1)
        current_null_rate = sum(
            1 for row in current_rows if row.feature_values.get(feature_name) in {None, ""}
        ) / max(len(current_rows), 1)
        if round(reference_null_rate, 6) != round(current_null_rate, 6):
            null_messages.append(
                f"{feature_name}: "
                f"reference={reference_null_rate:.3f}, current={current_null_rate:.3f}"
            )
    null_ok = not null_messages
    checks.append(
        ParityCheckResult(
            check_name="null_rate",
            passed=null_ok,
            message="null rates match" if null_ok else "; ".join(null_messages),
        )
    )
    passed = passed and null_ok

    row_count_ok = len(reference_rows) == len(current_rows)
    checks.append(
        ParityCheckResult(
            check_name="row_count",
            passed=row_count_ok,
            message=(
                "row counts match"
                if row_count_ok
                else (
                    "row counts differ: "
                    f"reference={len(reference_rows)}, current={len(current_rows)}"
                )
            ),
        )
    )
    passed = passed and row_count_ok

    if deterministic and row_count_ok:
        value_mismatches: list[str] = []
        for index, (reference_row, current_row) in enumerate(
            zip(reference_rows, current_rows, strict=True),
            start=1,
        ):
            for feature_name in shared_schema:
                if (
                    reference_row.feature_values[feature_name]
                    != current_row.feature_values[feature_name]
                ):
                    value_mismatches.append(
                        f"row {index}, feature {feature_name}: "
                        f"reference={reference_row.feature_values[feature_name]!r}, "
                        f"current={current_row.feature_values[feature_name]!r}"
                    )
                    if len(value_mismatches) >= 5:
                        break
            if len(value_mismatches) >= 5:
                break
        values_ok = not value_mismatches
        checks.append(
            ParityCheckResult(
                check_name="deterministic_values",
                passed=values_ok,
                message="deterministic values match" if values_ok else "; ".join(value_mismatches),
            )
        )
        passed = passed and values_ok

    return ParityReport(
        passed=passed,
        checks=checks,
        compared_rows=min(len(reference_rows), len(current_rows)),
    )


def save_report(report: ParityReport, output_path: Path) -> None:
    """Persist the parity report as JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    """Run the feature parity CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        validate_args(args)
        reference_rows = build_feature_rows_from_jsonl(args.reference)
        current_rows = build_feature_rows_from_jsonl(args.current)
        report = compare_feature_datasets(
            reference_rows,
            current_rows,
            deterministic=args.deterministic,
        )
        save_report(report, args.output)
    except ValueError as exc:
        parser.exit(status=2, message=f"{exc}\n")

    print(json.dumps(report.to_dict(), sort_keys=True))
    return 0 if report.passed else 1
