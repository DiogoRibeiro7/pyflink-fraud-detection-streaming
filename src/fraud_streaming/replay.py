"""Replay historical transaction streams to stdout, file, or Kafka."""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from fraud_streaming.kafka.producer import ProducerLike, create_producer, prepare_message
from fraud_streaming.schemas import Transaction
from fraud_streaming.serialization import (
    parse_event_time,
    transaction_from_json,
    transaction_to_json,
)


@dataclass(frozen=True, slots=True)
class ReplayPlanItem:
    """One replay item with its computed delay."""

    transaction: Transaction
    delay_seconds: float


@dataclass(frozen=True, slots=True)
class ReplayConfig:
    """Validated replay configuration."""

    input_path: Path
    output_mode: str
    output_path: Path | None
    bootstrap_servers: str | None
    topic: str | None
    key_field: str
    preserve_timing: bool
    speed_multiplier: float | None
    as_fast_as_possible: bool
    start_time: str | None
    end_time: str | None
    user_id: str | None
    card_id: str | None
    dry_run: bool


class ReplaySink(Protocol):
    """Minimal replay sink interface."""

    def emit(self, transaction: Transaction) -> None:
        """Emit one replayed transaction."""

    def close(self) -> None:
        """Flush and close the sink."""


class StdoutReplaySink:
    """Write replayed transactions as JSON lines to stdout."""

    def emit(self, transaction: Transaction) -> None:
        print(transaction_to_json(transaction))

    def close(self) -> None:
        return None


class FileReplaySink:
    """Write replayed transactions to a JSONL file."""

    def __init__(self, output_path: Path) -> None:
        self._output_path = output_path
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self._output_path.open("w", encoding="utf-8")

    def emit(self, transaction: Transaction) -> None:
        self._handle.write(transaction_to_json(transaction) + "\n")

    def close(self) -> None:
        self._handle.close()


class KafkaReplaySink:
    """Write replayed transactions to Kafka."""

    def __init__(self, producer: ProducerLike, topic: str, key_field: str) -> None:
        self._producer = producer
        self._topic = topic
        self._key_field = key_field

    def emit(self, transaction: Transaction) -> None:
        message = prepare_message(transaction, self._key_field)
        self._producer.send(self._topic, key=message.key, value=message.value)

    def close(self) -> None:
        self._producer.flush()


def build_parser() -> argparse.ArgumentParser:
    """Create the replay CLI parser."""
    parser = argparse.ArgumentParser(description="Replay historical transaction JSONL streams.")
    parser.add_argument("input", type=Path, help="Path to a JSONL transaction file.")
    parser.add_argument("--output-mode", choices=["stdout", "file", "kafka"], default="stdout")
    parser.add_argument("--output-path", type=Path, help="Output path when --output-mode=file.")
    parser.add_argument(
        "--bootstrap-servers",
        help="Kafka bootstrap servers when --output-mode=kafka.",
    )
    parser.add_argument("--topic", help="Kafka topic when --output-mode=kafka.")
    parser.add_argument(
        "--key-field",
        default="user_id",
        help="Kafka key field for replayed messages.",
    )
    parser.add_argument(
        "--preserve-timing",
        action="store_true",
        help="Preserve original event spacing.",
    )
    parser.add_argument(
        "--speed-multiplier",
        type=float,
        help="Replay faster than event time by this multiplier.",
    )
    parser.add_argument(
        "--as-fast-as-possible",
        action="store_true",
        help="Ignore event-time spacing and emit immediately.",
    )
    parser.add_argument("--start-time", help="Optional ISO-8601 inclusive lower event_time bound.")
    parser.add_argument("--end-time", help="Optional ISO-8601 inclusive upper event_time bound.")
    parser.add_argument("--user-id", help="Optional user_id filter.")
    parser.add_argument("--card-id", help="Optional card_id filter.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the replay plan without emitting.",
    )
    return parser


def validate_args(args: argparse.Namespace) -> ReplayConfig:
    """Validate replay arguments."""
    if not args.input.exists():
        raise ValueError(f"input file does not exist: {args.input}")
    if not args.input.is_file():
        raise ValueError(f"input path is not a file: {args.input}")
    if args.output_mode == "file" and args.output_path is None:
        raise ValueError("--output-path is required when --output-mode=file")
    if args.output_mode == "kafka":
        if not args.bootstrap_servers:
            raise ValueError("--bootstrap-servers is required when --output-mode=kafka")
        if not args.topic:
            raise ValueError("--topic is required when --output-mode=kafka")
    if args.speed_multiplier is not None and args.speed_multiplier <= 0:
        raise ValueError("--speed-multiplier must be positive")
    if args.preserve_timing and args.as_fast_as_possible:
        raise ValueError("--preserve-timing and --as-fast-as-possible are mutually exclusive")
    if args.start_time is not None:
        parse_event_time(args.start_time)
    if args.end_time is not None:
        parse_event_time(args.end_time)
    if (
        args.start_time
        and args.end_time
        and parse_event_time(args.start_time) > parse_event_time(args.end_time)
    ):
        raise ValueError("--start-time must be earlier than or equal to --end-time")

    return ReplayConfig(
        input_path=args.input,
        output_mode=args.output_mode,
        output_path=args.output_path,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        key_field=args.key_field,
        preserve_timing=args.preserve_timing,
        speed_multiplier=args.speed_multiplier,
        as_fast_as_possible=args.as_fast_as_possible,
        start_time=args.start_time,
        end_time=args.end_time,
        user_id=args.user_id,
        card_id=args.card_id,
        dry_run=args.dry_run,
    )


def load_transactions(input_path: Path) -> list[Transaction]:
    """Load transactions from a JSONL file."""
    transactions: list[Transaction] = []
    with input_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                transactions.append(transaction_from_json(line))
            except (TypeError, ValueError) as exc:
                raise ValueError(f"invalid transaction at line {line_number}: {exc}") from exc
    return transactions


def filter_transactions(
    transactions: Iterable[Transaction],
    *,
    start_time: str | None = None,
    end_time: str | None = None,
    user_id: str | None = None,
    card_id: str | None = None,
) -> list[Transaction]:
    """Filter transactions by time range and identity."""
    start_dt = parse_event_time(start_time) if start_time is not None else None
    end_dt = parse_event_time(end_time) if end_time is not None else None
    filtered: list[Transaction] = []
    for transaction in transactions:
        if start_dt is not None and transaction.event_time < start_dt:
            continue
        if end_dt is not None and transaction.event_time > end_dt:
            continue
        if user_id is not None and transaction.user_id != user_id:
            continue
        if card_id is not None and transaction.card_id != card_id:
            continue
        filtered.append(transaction)
    return filtered


def plan_replay(
    transactions: list[Transaction],
    *,
    preserve_timing: bool = False,
    speed_multiplier: float | None = None,
    as_fast_as_possible: bool = False,
) -> list[ReplayPlanItem]:
    """Build a deterministic replay plan with computed delays."""
    if as_fast_as_possible or not transactions:
        return [
            ReplayPlanItem(transaction=transaction, delay_seconds=0.0)
            for transaction in transactions
        ]

    if not preserve_timing and speed_multiplier is None:
        return [
            ReplayPlanItem(transaction=transaction, delay_seconds=0.0)
            for transaction in transactions
        ]

    effective_multiplier = 1.0 if preserve_timing else speed_multiplier
    if effective_multiplier is None:
        effective_multiplier = 1.0

    plan: list[ReplayPlanItem] = []
    previous_time = transactions[0].event_time
    plan.append(ReplayPlanItem(transaction=transactions[0], delay_seconds=0.0))
    for transaction in transactions[1:]:
        raw_delta = (transaction.event_time - previous_time).total_seconds()
        delay_seconds = max(raw_delta / effective_multiplier, 0.0)
        plan.append(ReplayPlanItem(transaction=transaction, delay_seconds=delay_seconds))
        previous_time = transaction.event_time
    return plan


def create_replay_sink(config: ReplayConfig) -> ReplaySink:
    """Create the requested replay sink."""
    if config.output_mode == "stdout":
        return StdoutReplaySink()
    if config.output_mode == "file":
        if config.output_path is None:
            raise ValueError("output_path is required for file replay")
        return FileReplaySink(config.output_path)
    if config.output_mode == "kafka":
        if config.bootstrap_servers is None or config.topic is None:
            raise ValueError("Kafka replay requires bootstrap_servers and topic")
        producer = create_producer(config.bootstrap_servers)
        return KafkaReplaySink(producer, config.topic, config.key_field)
    raise ValueError(f"unsupported output mode: {config.output_mode}")


def execute_replay(
    plan: list[ReplayPlanItem],
    sink: ReplaySink,
    *,
    sleep_fn: Callable[[float], object] = time.sleep,
    dry_run: bool = False,
) -> int:
    """Execute a replay plan against the chosen sink."""
    emitted = 0
    try:
        for item in plan:
            if item.delay_seconds > 0:
                sleep_fn(item.delay_seconds)
            if dry_run:
                print(
                    f"delay={item.delay_seconds:.3f}s "
                    f"transaction_id={item.transaction.transaction_id} "
                    f"user_id={item.transaction.user_id} "
                    f"event_time={item.transaction.event_time.isoformat()}"
                )
            else:
                sink.emit(item.transaction)
            emitted += 1
    finally:
        sink.close()
    return emitted


def main(argv: Sequence[str] | None = None) -> int:
    """Run the replay CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        config = validate_args(args)
        loaded = load_transactions(config.input_path)
        filtered = filter_transactions(
            loaded,
            start_time=config.start_time,
            end_time=config.end_time,
            user_id=config.user_id,
            card_id=config.card_id,
        )
        plan = plan_replay(
            filtered,
            preserve_timing=config.preserve_timing,
            speed_multiplier=config.speed_multiplier,
            as_fast_as_possible=config.as_fast_as_possible,
        )
        sink = create_replay_sink(config)
        emitted = execute_replay(plan, sink, dry_run=config.dry_run)
    except (RuntimeError, ValueError) as exc:
        parser.exit(status=2, message=f"{exc}\n")

    print(f"Replayed {emitted} transaction events.")
    return 0
