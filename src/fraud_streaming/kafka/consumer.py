"""Kafka fraud alert consumer for local demos."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import Protocol, cast

from fraud_streaming.schemas import Alert, RiskLevel
from fraud_streaming.serialization import alert_from_json, alert_to_json


class ConsumerLike(Protocol):
    """Minimal Kafka consumer protocol used by the demo CLI."""

    def __iter__(self) -> Iterator[object]:
        """Iterate over consumed messages."""

    def close(self) -> None:
        """Close the consumer."""


class ConsumerFactory(Protocol):
    """Callable factory for optional Kafka consumer instances."""

    def __call__(
        self,
        topic: str,
        *,
        bootstrap_servers: str,
        group_id: str,
        auto_offset_reset: str,
        consumer_timeout_ms: int,
    ) -> ConsumerLike:
        """Create a consumer instance."""


@dataclass(frozen=True, slots=True)
class ConsumerSummary:
    """Summary counts for consumed alerts."""

    total: int
    by_risk_level: dict[RiskLevel, int]


def _require_kafka_consumer_class() -> ConsumerFactory:
    """Import the optional Kafka consumer dependency."""
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise RuntimeError(
            "Kafka support is not installed. Install it with "
            "`poetry install --with dev -E kafka` or `pip install kafka-python`."
        ) from exc
    return cast(ConsumerFactory, KafkaConsumer)


def build_parser() -> argparse.ArgumentParser:
    """Create the Kafka alert consumer CLI parser."""
    parser = argparse.ArgumentParser(description="Consume fraud alerts from a Kafka topic.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="fraud-alerts")
    parser.add_argument("--group-id", default="fraud-alert-consumer")
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--max-messages", type=int)
    parser.add_argument("--risk-level", choices=["low", "elevated", "medium", "high"])
    parser.add_argument("--min-risk-score", type=int)
    parser.add_argument("--summary", action="store_true")
    return parser


def validate_args(args: argparse.Namespace) -> None:
    """Validate CLI arguments before attempting Kafka work."""
    if not args.bootstrap_servers.strip():
        raise ValueError("--bootstrap-servers cannot be empty")
    if not args.topic.strip():
        raise ValueError("--topic cannot be empty")
    if not args.group_id.strip():
        raise ValueError("--group-id cannot be empty")
    if args.max_messages is not None and args.max_messages <= 0:
        raise ValueError("--max-messages must be positive")
    if args.min_risk_score is not None and not 0 <= args.min_risk_score <= 100:
        raise ValueError("--min-risk-score must be between 0 and 100")


def create_consumer(args: argparse.Namespace) -> ConsumerLike:
    """Create a Kafka consumer instance."""
    consumer_class = _require_kafka_consumer_class()
    timeout_ms = 1_000 if args.max_messages is not None else 0
    offset_reset = "earliest" if args.from_beginning else "latest"
    return consumer_class(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset=offset_reset,
        consumer_timeout_ms=timeout_ms,
    )


def parse_alert_message(raw_value: bytes | str) -> Alert:
    """Parse one Kafka payload into an Alert."""
    if isinstance(raw_value, bytes):
        payload = raw_value.decode("utf-8")
    elif isinstance(raw_value, str):
        payload = raw_value
    else:
        raise TypeError("Kafka message value must be bytes or string")
    return alert_from_json(payload)


def alert_matches_filters(
    alert: Alert,
    risk_level: RiskLevel | None = None,
    min_risk_score: int | None = None,
) -> bool:
    """Return whether an alert matches the requested filters."""
    if risk_level is not None and alert.risk_level != risk_level:
        return False
    return min_risk_score is None or alert.risk_score >= min_risk_score


def summarize_alerts(alerts: Iterable[Alert]) -> ConsumerSummary:
    """Aggregate consumed alerts by risk level."""
    counter: Counter[RiskLevel] = Counter()
    total = 0
    for alert in alerts:
        counter[alert.risk_level] += 1
        total += 1
    return ConsumerSummary(
        total=total,
        by_risk_level={
            "low": counter.get("low", 0),
            "elevated": counter.get("elevated", 0),
            "medium": counter.get("medium", 0),
            "high": counter.get("high", 0),
        },
    )


def consume_alerts(
    consumer: ConsumerLike,
    risk_level: RiskLevel | None = None,
    min_risk_score: int | None = None,
    max_messages: int | None = None,
) -> list[Alert]:
    """Consume, parse, and filter alerts."""
    matched: list[Alert] = []

    try:
        for record in consumer:
            raw_value = getattr(record, "value", record)
            if not isinstance(raw_value, bytes | str):
                raise TypeError("Kafka message value must be bytes or string")
            alert = parse_alert_message(raw_value)
            if not alert_matches_filters(
                alert,
                risk_level=risk_level,
                min_risk_score=min_risk_score,
            ):
                continue
            matched.append(alert)
            if max_messages is not None and len(matched) >= max_messages:
                break
    finally:
        consumer.close()

    return matched


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Kafka fraud alert consumer CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        validate_args(args)
        consumer = create_consumer(args)
        alerts = consume_alerts(
            consumer=consumer,
            risk_level=args.risk_level,
            min_risk_score=args.min_risk_score,
            max_messages=args.max_messages,
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        parser.exit(status=2, message=f"{exc}\n")

    for alert in alerts:
        print(alert_to_json(alert))

    if args.summary:
        summary = summarize_alerts(alerts)
        print(
            json.dumps(
                {"total": summary.total, "by_risk_level": summary.by_risk_level},
                sort_keys=True,
            )
        )

    return 0
