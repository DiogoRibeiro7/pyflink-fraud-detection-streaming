from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "benchmarks" / "benchmark_local_runner.py"
SPEC = importlib.util.spec_from_file_location("benchmark_local_runner", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load benchmark module from {MODULE_PATH}")
BENCHMARK_MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = BENCHMARK_MODULE
SPEC.loader.exec_module(BENCHMARK_MODULE)

BenchmarkReport = BENCHMARK_MODULE.BenchmarkReport
BenchmarkResult = BENCHMARK_MODULE.BenchmarkResult
build_report = BENCHMARK_MODULE.build_report
render_markdown = BENCHMARK_MODULE.render_markdown
save_report = BENCHMARK_MODULE.save_report
scenario_matrix = BENCHMARK_MODULE.scenario_matrix
validate_args = BENCHMARK_MODULE.validate_args


def make_args(**overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "users_list": [5],
        "transaction_list": [20],
        "seed": 42,
        "repeats": 1,
        "output": Path("artifacts/test-benchmark.json"),
        "markdown_output": None,
        "include_kafka_producer": False,
        "disable_memory": True,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_validate_args_rejects_non_positive_inputs() -> None:
    with pytest.raises(ValueError, match="--users must be positive"):
        validate_args(make_args(users_list=[0]))

    with pytest.raises(ValueError, match="--transactions must be positive"):
        validate_args(make_args(transaction_list=[-1]))

    with pytest.raises(ValueError, match="--repeats must be positive"):
        validate_args(make_args(repeats=0))


def test_scenario_matrix_builds_cross_product() -> None:
    scenarios = scenario_matrix(make_args(users_list=[2, 3], transaction_list=[10, 20]))

    assert [(item.users, item.transactions) for item in scenarios] == [
        (2, 10),
        (2, 20),
        (3, 10),
        (3, 20),
    ]


def test_build_report_runs_small_local_benchmark() -> None:
    report = build_report(make_args(users_list=[2], transaction_list=[5], repeats=1))

    assert len(report.results) == 1
    result = report.results[0]
    assert result.mode == "local_runner"
    assert result.transactions == 5
    assert result.events_per_second > 0


def test_render_markdown_contains_benchmark_table() -> None:
    report = BenchmarkReport(
        created_at="2026-06-11T12:00:00+00:00",
        python_version="3.12.0",
        include_kafka_producer=False,
        results=[
            BenchmarkResult(
                mode="local_runner",
                scenario="users-5_transactions-20",
                users=5,
                transactions=20,
                repeats=1,
                emitted_alerts=3,
                average_duration_seconds=0.2,
                fastest_duration_seconds=0.2,
                slowest_duration_seconds=0.2,
                events_per_second=100.0,
                peak_memory_bytes=1_048_576,
            )
        ],
    )

    markdown = render_markdown(report)

    assert "| Mode | Scenario | Transactions |" in markdown
    assert "local_runner" in markdown
    assert "100.00" in markdown


def test_save_report_writes_json_file(tmp_path: Path) -> None:
    output = tmp_path / "benchmark.json"
    report = BenchmarkReport(
        created_at="2026-06-11T12:00:00+00:00",
        python_version="3.12.0",
        include_kafka_producer=False,
        results=[],
    )

    save_report(report, output)

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["created_at"] == "2026-06-11T12:00:00+00:00"
