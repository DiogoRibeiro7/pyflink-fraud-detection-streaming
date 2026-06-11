"""Benchmark the pure-Python fraud pipeline and optional Kafka preparation path."""

from __future__ import annotations

import argparse
import json
import platform
import sys
import tracemalloc
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from time import perf_counter

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fraud_streaming.kafka.producer import (
    ProducerLike,
    _require_kafka_producer_class,
    iter_generated_transactions,
    publish_transactions,
)
from fraud_streaming.local_runner import process_json_lines
from fraud_streaming.synthetic import generate_transactions


@dataclass(frozen=True, slots=True)
class BenchmarkScenario:
    """One benchmark scenario specification."""

    users: int
    transactions: int

    @property
    def name(self) -> str:
        """Return a stable scenario name."""
        return f"users-{self.users}_transactions-{self.transactions}"


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    """Metrics for one benchmark scenario."""

    mode: str
    scenario: str
    users: int
    transactions: int
    repeats: int
    emitted_alerts: int
    average_duration_seconds: float
    fastest_duration_seconds: float
    slowest_duration_seconds: float
    events_per_second: float
    peak_memory_bytes: int | None
    message_bytes_total: int | None = None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-compatible representation."""
        return {
            "mode": self.mode,
            "scenario": self.scenario,
            "users": self.users,
            "transactions": self.transactions,
            "repeats": self.repeats,
            "emitted_alerts": self.emitted_alerts,
            "average_duration_seconds": self.average_duration_seconds,
            "fastest_duration_seconds": self.fastest_duration_seconds,
            "slowest_duration_seconds": self.slowest_duration_seconds,
            "events_per_second": self.events_per_second,
            "peak_memory_bytes": self.peak_memory_bytes,
            "message_bytes_total": self.message_bytes_total,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkReport:
    """Aggregate report saved by the benchmark script."""

    created_at: str
    python_version: str
    include_kafka_producer: bool
    results: list[BenchmarkResult]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-compatible representation."""
        return {
            "created_at": self.created_at,
            "python_version": self.python_version,
            "include_kafka_producer": self.include_kafka_producer,
            "results": [result.to_dict() for result in self.results],
        }


class _BenchmarkProducer:
    """In-memory producer used to measure producer serialization overhead."""

    def __init__(self) -> None:
        self.message_bytes_total = 0

    def send(self, topic: str, key: bytes, value: bytes) -> object:
        del topic
        self.message_bytes_total += len(key) + len(value)
        return object()

    def flush(self) -> None:
        return None


def build_parser() -> argparse.ArgumentParser:
    """Create the benchmark script argument parser."""
    parser = argparse.ArgumentParser(description="Benchmark the local fraud runner.")
    parser.add_argument(
        "--users",
        type=int,
        action="append",
        dest="users_list",
        help="User count for one scenario. Repeat the flag to benchmark multiple sizes.",
    )
    parser.add_argument(
        "--transactions",
        type=int,
        action="append",
        dest="transaction_list",
        help="Transaction count for one scenario. Repeat the flag to benchmark multiple sizes.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Synthetic data generation seed.")
    parser.add_argument(
        "--repeats",
        type=int,
        default=3,
        help="How many timed repeats to run per scenario.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/benchmarks/local_runner_benchmark.json"),
        help="Path where the JSON benchmark report will be written.",
    )
    parser.add_argument(
        "--markdown-output",
        type=Path,
        help="Optional Markdown summary path.",
    )
    parser.add_argument(
        "--include-kafka-producer",
        action="store_true",
        help=(
            "Also benchmark the Kafka producer preparation path when the optional "
            "Kafka dependency is installed."
        ),
    )
    parser.add_argument(
        "--disable-memory",
        action="store_true",
        help="Disable tracemalloc-based peak memory measurement.",
    )
    return parser


def validate_args(args: argparse.Namespace) -> None:
    """Validate benchmark CLI arguments."""
    users_list = [10] if args.users_list is None else args.users_list
    transaction_list = [1_000] if args.transaction_list is None else args.transaction_list
    if any(value <= 0 for value in users_list):
        raise ValueError("--users must be positive")
    if any(value <= 0 for value in transaction_list):
        raise ValueError("--transactions must be positive")
    if args.repeats <= 0:
        raise ValueError("--repeats must be positive")
    if args.output.exists() and args.output.is_dir():
        raise ValueError(f"output path is a directory: {args.output}")
    if args.markdown_output is not None and args.markdown_output.exists() and args.markdown_output.is_dir():
        raise ValueError(f"markdown output path is a directory: {args.markdown_output}")
    if args.include_kafka_producer:
        try:
            _require_kafka_producer_class()
        except RuntimeError as exc:
            raise ValueError(str(exc)) from exc


def scenario_matrix(args: argparse.Namespace) -> list[BenchmarkScenario]:
    """Build the scenario matrix from CLI arguments."""
    users_list = [10] if args.users_list is None else args.users_list
    transaction_list = [1_000] if args.transaction_list is None else args.transaction_list
    return [
        BenchmarkScenario(users=users, transactions=transactions)
        for users in users_list
        for transactions in transaction_list
    ]


def _measure_peak_memory(enabled: bool, run: callable[[], object]) -> tuple[object, int | None]:
    """Run a callable and optionally capture tracemalloc peak bytes."""
    if not enabled:
        return run(), None
    tracemalloc.start()
    try:
        result = run()
        _, peak = tracemalloc.get_traced_memory()
        return result, peak
    finally:
        tracemalloc.stop()


def _prepare_local_lines(scenario: BenchmarkScenario, seed: int) -> list[str]:
    """Generate synthetic JSON lines for one local-runner scenario."""
    payloads = generate_transactions(
        users=scenario.users,
        transactions=scenario.transactions,
        seed=seed,
    )
    return [json.dumps(payload, separators=(",", ":"), sort_keys=True) for payload in payloads]


def run_local_benchmark(
    scenario: BenchmarkScenario,
    *,
    seed: int,
    repeats: int,
    measure_memory: bool,
) -> BenchmarkResult:
    """Benchmark the pure-Python local runner on one synthetic scenario."""
    lines = _prepare_local_lines(scenario, seed)
    durations: list[float] = []
    emitted_alerts = 0
    peak_memory_bytes: int | None = None

    for repeat_index in range(repeats):
        def _run() -> int:
            return sum(1 for _ in process_json_lines(lines))

        start = perf_counter()
        result, peak = _measure_peak_memory(measure_memory and repeat_index == 0, _run)
        duration = perf_counter() - start
        durations.append(duration)
        emitted_alerts = int(result)
        if peak is not None:
            peak_memory_bytes = peak

    average_duration = mean(durations)
    return BenchmarkResult(
        mode="local_runner",
        scenario=scenario.name,
        users=scenario.users,
        transactions=scenario.transactions,
        repeats=repeats,
        emitted_alerts=emitted_alerts,
        average_duration_seconds=average_duration,
        fastest_duration_seconds=min(durations),
        slowest_duration_seconds=max(durations),
        events_per_second=(scenario.transactions / average_duration) if average_duration else 0.0,
        peak_memory_bytes=peak_memory_bytes,
    )


def run_kafka_producer_benchmark(
    scenario: BenchmarkScenario,
    *,
    seed: int,
    repeats: int,
    measure_memory: bool,
) -> BenchmarkResult:
    """Benchmark the Kafka producer preparation path without requiring a broker."""
    durations: list[float] = []
    emitted_alerts = 0
    peak_memory_bytes: int | None = None
    message_bytes_total = 0

    for repeat_index in range(repeats):
        producer = _BenchmarkProducer()

        def _run() -> int:
            return publish_transactions(
                producer=producer,
                topic="benchmark-transactions",
                transactions=iter_generated_transactions(
                    users=scenario.users,
                    transactions=scenario.transactions,
                    seed=seed,
                ),
                key_field="user_id",
                sleep_ms=0,
            )

        start = perf_counter()
        result, peak = _measure_peak_memory(measure_memory and repeat_index == 0, _run)
        duration = perf_counter() - start
        durations.append(duration)
        emitted_alerts = int(result)
        message_bytes_total = producer.message_bytes_total
        if peak is not None:
            peak_memory_bytes = peak

    average_duration = mean(durations)
    return BenchmarkResult(
        mode="kafka_producer_prepare",
        scenario=scenario.name,
        users=scenario.users,
        transactions=scenario.transactions,
        repeats=repeats,
        emitted_alerts=emitted_alerts,
        average_duration_seconds=average_duration,
        fastest_duration_seconds=min(durations),
        slowest_duration_seconds=max(durations),
        events_per_second=(scenario.transactions / average_duration) if average_duration else 0.0,
        peak_memory_bytes=peak_memory_bytes,
        message_bytes_total=message_bytes_total,
    )


def build_report(args: argparse.Namespace) -> BenchmarkReport:
    """Run the requested benchmark scenarios and build a report."""
    results: list[BenchmarkResult] = []
    scenarios = scenario_matrix(args)
    for scenario in scenarios:
        results.append(
            run_local_benchmark(
                scenario,
                seed=args.seed,
                repeats=args.repeats,
                measure_memory=not args.disable_memory,
            )
        )
        if args.include_kafka_producer:
            results.append(
                run_kafka_producer_benchmark(
                    scenario,
                    seed=args.seed,
                    repeats=args.repeats,
                    measure_memory=not args.disable_memory,
                )
            )

    return BenchmarkReport(
        created_at=datetime.now(timezone.utc).isoformat(),
        python_version=platform.python_version(),
        include_kafka_producer=args.include_kafka_producer,
        results=results,
    )


def save_report(report: BenchmarkReport, output_path: Path) -> None:
    """Write the benchmark report as JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


def render_markdown(report: BenchmarkReport) -> str:
    """Render a compact Markdown summary for benchmark results."""
    lines = [
        "# Benchmark Report",
        "",
        f"- Created at: `{report.created_at}`",
        f"- Python: `{report.python_version}`",
        f"- Kafka producer preparation included: `{report.include_kafka_producer}`",
        "",
        "| Mode | Scenario | Transactions | Alerts/Msgs | Avg Seconds | Events/Sec | Peak Memory MiB |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for result in report.results:
        peak_memory_mib = (
            f"{result.peak_memory_bytes / (1024 * 1024):.2f}"
            if result.peak_memory_bytes is not None
            else "n/a"
        )
        lines.append(
            "| "
            f"{result.mode} | {result.scenario} | {result.transactions} | "
            f"{result.emitted_alerts} | {result.average_duration_seconds:.6f} | "
            f"{result.events_per_second:.2f} | {peak_memory_mib} |"
        )

    lines.extend(
        [
            "",
            "Notes:",
            "- `local_runner` measures the existing pure-Python fraud pipeline only.",
            "- `kafka_producer_prepare` measures serialization and send-loop overhead with an in-memory producer stub. It is not a real broker throughput benchmark.",
            "- Benchmarks are intentionally separate from unit tests and CI smoke coverage.",
        ]
    )
    return "\n".join(lines)


def save_markdown(report: BenchmarkReport, output_path: Path) -> None:
    """Write the Markdown benchmark summary."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_markdown(report), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the benchmark script."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        validate_args(args)
        report = build_report(args)
        save_report(report, args.output)
        if args.markdown_output is not None:
            save_markdown(report, args.markdown_output)
    except ValueError as exc:
        parser.exit(status=2, message=f"{exc}\n")

    print(f"Saved benchmark report to {args.output}")
    if args.markdown_output is not None:
        print(f"Saved benchmark Markdown summary to {args.markdown_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
