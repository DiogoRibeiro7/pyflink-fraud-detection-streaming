"""Command-line interface for local fraud detection demos."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from fraud_streaming.local_runner import process_json_lines
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

    with input_path.open("r", encoding="utf-8") as handle:
        for alert in process_json_lines(handle, emit_low_risk=args.show_all):
            print(alert_to_json(alert))

    return 0


if __name__ == "__main__":
    sys.exit(main())
