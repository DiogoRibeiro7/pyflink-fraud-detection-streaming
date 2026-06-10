"""Kafka transaction producer for local demos."""

from __future__ import annotations

import argparse
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Protocol, cast

from fraud_streaming.schemas import Transaction
from fraud_streaming.serialization import (
    transaction_from_dict,
    transaction_from_json,
    transaction_to_json,
)
from fraud_streaming.synthetic import generate_transactions

VALID_KEY_FIELDS = frozenset(field.name for field in fields(Transaction))


class ProducerLike(Protocol):
    """Minimal Kafka producer protocol used by the demo CLI."""

    def send(self, topic: str, key: bytes, value: bytes) -> object:
        """Publish one message."""

    def flush(self) -> None:
        """Flush pending messages."""


class ProducerFactory(Protocol):
    """Callable factory for optional Kafka producer instances."""

    def __call__(
        self,
        *,
        bootstrap_servers: str,
        key_serializer: None = None,
        value_serializer: None = None,
    ) -> ProducerLike:
        """Create a producer instance."""


@dataclass(frozen=True, slots=True)
class PreparedMessage:
    """Serialized Kafka message ready to publish."""

    key: bytes
    value: bytes


def _require_kafka_producer_class() -> ProducerFactory:
    """Import the optional Kafka producer dependency."""
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError(
            "Kafka support is not installed. Install it with "
            "`poetry install --with dev -E kafka` or `pip install kafka-python`."
        ) from exc
    return cast(ProducerFactory, KafkaProducer)


def build_parser() -> argparse.ArgumentParser:
    """Create the Kafka producer CLI parser."""
    parser = argparse.ArgumentParser(description="Publish transaction events to a Kafka topic.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--input", type=Path, help="Path to a JSONL transaction file.")
    parser.add_argument("--users", type=int, default=10)
    parser.add_argument("--transactions", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--key-field", default="user_id")
    return parser


def validate_args(args: argparse.Namespace) -> None:
    """Validate CLI arguments before attempting Kafka work."""
    if not args.bootstrap_servers.strip():
        raise ValueError("--bootstrap-servers cannot be empty")
    if not args.topic.strip():
        raise ValueError("--topic cannot be empty")
    if args.sleep_ms < 0:
        raise ValueError("--sleep-ms must be non-negative")
    if args.key_field not in VALID_KEY_FIELDS:
        valid = ", ".join(sorted(VALID_KEY_FIELDS))
        raise ValueError(f"--key-field must be one of: {valid}")

    if args.input is not None:
        if not args.input.exists():
            raise ValueError(f"input file does not exist: {args.input}")
        if not args.input.is_file():
            raise ValueError(f"input path is not a file: {args.input}")
        return

    if args.users <= 0:
        raise ValueError("--users must be positive")
    if args.transactions <= 0:
        raise ValueError("--transactions must be positive")


def iter_transactions_from_file(input_path: Path) -> Iterator[Transaction]:
    """Yield validated transactions from a JSONL file."""
    with input_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                yield transaction_from_json(line)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"invalid transaction at line {line_number}: {exc}") from exc


def iter_generated_transactions(users: int, transactions: int, seed: int) -> Iterator[Transaction]:
    """Yield validated synthetic transactions."""
    for payload in generate_transactions(users=users, transactions=transactions, seed=seed):
        yield transaction_from_dict(payload)


def iter_transactions(args: argparse.Namespace) -> Iterator[Transaction]:
    """Yield transactions from either file or synthetic generation mode."""
    if args.input is not None:
        yield from iter_transactions_from_file(args.input)
        return
    yield from iter_generated_transactions(args.users, args.transactions, args.seed)


def prepare_message(transaction: Transaction, key_field: str) -> PreparedMessage:
    """Serialize one transaction into a Kafka message."""
    if key_field not in VALID_KEY_FIELDS:
        valid = ", ".join(sorted(VALID_KEY_FIELDS))
        raise ValueError(f"unsupported key field '{key_field}'. Expected one of: {valid}")

    key_value = getattr(transaction, key_field)
    if key_value is None:
        raise ValueError(f"transaction field '{key_field}' cannot be null when used as a key")

    return PreparedMessage(
        key=str(key_value).encode("utf-8"),
        value=transaction_to_json(transaction).encode("utf-8"),
    )


def create_producer(bootstrap_servers: str) -> ProducerLike:
    """Create a Kafka producer instance."""
    producer_class = _require_kafka_producer_class()
    return producer_class(
        bootstrap_servers=bootstrap_servers,
        key_serializer=None,
        value_serializer=None,
    )


def publish_transactions(
    producer: ProducerLike,
    topic: str,
    transactions: Iterator[Transaction],
    key_field: str,
    sleep_ms: int = 0,
) -> int:
    """Publish transactions to Kafka and return the sent count."""
    count = 0
    for transaction in transactions:
        message = prepare_message(transaction, key_field)
        producer.send(topic, key=message.key, value=message.value)
        count += 1
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1_000)

    producer.flush()
    return count


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Kafka transaction producer CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        validate_args(args)
        producer = create_producer(args.bootstrap_servers)
        count = publish_transactions(
            producer=producer,
            topic=args.topic,
            transactions=iter_transactions(args),
            key_field=args.key_field,
            sleep_ms=args.sleep_ms,
        )
    except (RuntimeError, ValueError) as exc:
        parser.exit(status=2, message=f"{exc}\n")

    print(f"Published {count} transaction events to topic '{args.topic}'.")
    return 0
