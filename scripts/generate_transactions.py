#!/usr/bin/env python
"""Generate synthetic transaction events for local demos."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fraud_streaming.synthetic import generate_transactions  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser."""
    parser = argparse.ArgumentParser(description="Generate synthetic JSONL transactions.")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--users", type=int, default=10)
    parser.add_argument("--transactions", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the generator."""
    parser = build_parser()
    args = parser.parse_args(argv)

    events = generate_transactions(args.users, args.transactions, args.seed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, sort_keys=True) + "\n")

    print(f"Wrote {len(events)} transactions to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
