#!/usr/bin/env python
"""Generate synthetic transaction events for local demos."""

from __future__ import annotations

import argparse
import json
import random
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path

COUNTRIES = ["PT", "ES", "FR", "DE", "GB", "US", "NL"]
CATEGORIES = ["grocery", "fuel", "restaurant", "travel", "electronics", "gaming", "atm"]
CHANNELS = ["pos", "online", "atm"]


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser."""
    parser = argparse.ArgumentParser(description="Generate synthetic JSONL transactions.")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--users", type=int, default=10)
    parser.add_argument("--transactions", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    return parser


def _normal_amount(category: str, rng: random.Random) -> float:
    """Generate a realistic-ish amount by merchant category."""
    if category == "grocery":
        return round(rng.uniform(8, 120), 2)
    if category == "fuel":
        return round(rng.uniform(30, 120), 2)
    if category == "restaurant":
        return round(rng.uniform(12, 180), 2)
    if category == "travel":
        return round(rng.uniform(50, 700), 2)
    if category == "electronics":
        return round(rng.uniform(40, 950), 2)
    if category == "gaming":
        return round(rng.uniform(5, 160), 2)
    return round(rng.uniform(20, 300), 2)


def generate_transactions(users: int, transactions: int, seed: int) -> list[dict[str, object]]:
    """Generate synthetic transactions with a few injected fraud-like behaviours."""
    if users <= 0:
        raise ValueError("users must be positive")
    if transactions <= 0:
        raise ValueError("transactions must be positive")

    rng = random.Random(seed)
    start = datetime(2026, 6, 10, 8, 0, tzinfo=timezone.utc)
    events: list[dict[str, object]] = []

    for index in range(transactions):
        user_number = rng.randint(1, users)
        category = rng.choice(CATEGORIES)
        country = "PT" if rng.random() < 0.82 else rng.choice(COUNTRIES)
        channel = rng.choice(CHANNELS)
        event_time = start + timedelta(minutes=index * rng.uniform(0.4, 2.0))
        amount = _normal_amount(category, rng)

        # Inject a few clustered high-risk events for one user/card.
        if index in {70, 71, 72, 73, 74, 75}:
            user_number = 1
            country = "US"
            category = "electronics"
            channel = "online"
            amount = round(rng.uniform(700, 1_500), 2)
            event_time = start + timedelta(hours=2, minutes=index - 70)

        device_id = (
            f"device-{user_number:03d}"
            if rng.random() < 0.9
            else f"device-{rng.randint(1, users):03d}"
        )

        events.append(
            {
                "transaction_id": f"tx-{index + 1:06d}",
                "user_id": f"user-{user_number:03d}",
                "card_id": f"card-{user_number:03d}",
                "merchant_id": f"merchant-{rng.randint(1, 80):03d}",
                "amount": amount,
                "currency": "EUR",
                "country": country,
                "device_id": device_id,
                "merchant_category": category,
                "event_time": event_time.isoformat().replace("+00:00", "Z"),
                "channel": channel,
                "is_card_present": channel != "online",
                "latitude": round(rng.uniform(37.0, 42.0), 6),
                "longitude": round(rng.uniform(-9.5, -6.0), 6),
            }
        )

    events.sort(key=lambda item: str(item["event_time"]))
    return events


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
