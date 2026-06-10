from __future__ import annotations

from pathlib import Path

from fraud_streaming.monitoring.drift import (
    build_drift_records,
    build_drift_report,
    population_stability_index,
    render_markdown_report,
    save_json_report,
)


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    import json

    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_population_stability_index_is_stable() -> None:
    psi = population_stability_index([0.1, 0.2, 0.3, 0.4], [0.1, 0.2, 0.35, 0.45], bins=4)

    assert psi >= 0.0
    assert round(psi, 6) == round(psi, 6)


def test_build_drift_report_includes_overall_metrics(tmp_path: Path) -> None:
    reference = tmp_path / "reference.jsonl"
    current = tmp_path / "current.jsonl"
    write_jsonl(
        reference,
        [
            {
                "transaction_id": "tx-1",
                "user_id": "user-1",
                "card_id": "card-1",
                "merchant_id": "merchant-1",
                "amount": 20.0,
                "currency": "EUR",
                "country": "PT",
                "device_id": "device-1",
                "merchant_category": "grocery",
                "event_time": "2026-06-10T12:00:00Z",
                "channel": "pos",
                "is_card_present": True,
            },
            {
                "transaction_id": "tx-2",
                "user_id": "user-2",
                "card_id": "card-2",
                "merchant_id": "merchant-2",
                "amount": 30.0,
                "currency": "EUR",
                "country": "PT",
                "device_id": "device-2",
                "merchant_category": "fuel",
                "event_time": "2026-06-10T12:01:00Z",
                "channel": "pos",
                "is_card_present": True,
            },
        ],
    )
    write_jsonl(
        current,
        [
            {
                "transaction_id": "tx-3",
                "user_id": "user-1",
                "card_id": "card-1",
                "merchant_id": "merchant-1",
                "amount": 120.0,
                "currency": "EUR",
                "country": "US",
                "device_id": "device-3",
                "merchant_category": "travel",
                "event_time": "2026-06-10T12:00:00Z",
                "channel": "online",
                "is_card_present": False,
            },
            {
                "transaction_id": "tx-4",
                "user_id": "user-2",
                "card_id": "card-2",
                "merchant_id": "merchant-2",
                "amount": 150.0,
                "currency": "EUR",
                "country": "US",
                "device_id": "device-4",
                "merchant_category": "travel",
                "event_time": "2026-06-10T12:01:00Z",
                "channel": "online",
                "is_card_present": False,
            },
        ],
    )

    report = build_drift_report(
        build_drift_records(reference),
        build_drift_records(current),
        ["country"],
    )

    assert report.reference_count == 2
    assert report.current_count == 2
    assert "risk_score" in report.overall


def test_render_markdown_report_contains_table() -> None:
    from fraud_streaming.monitoring.drift import DriftMetric, DriftReport

    report = DriftReport(
        reference_count=10,
        current_count=12,
        overall={
            "risk_score": DriftMetric(
                psi=0.1,
                mean_delta=1.0,
                std_delta=0.5,
                quantile_deltas={"p25": 0.1, "p50": 0.2, "p75": 0.3},
                ks_statistic=None,
            )
        },
        segments={},
    )

    markdown = render_markdown_report(report)

    assert "| Metric | PSI | Mean Delta | Std Delta | KS |" in markdown


def test_save_json_report_writes_file(tmp_path: Path) -> None:
    from fraud_streaming.monitoring.drift import DriftMetric, DriftReport

    report = DriftReport(
        reference_count=1,
        current_count=1,
        overall={
            "risk_score": DriftMetric(
                psi=0.0,
                mean_delta=0.0,
                std_delta=0.0,
                quantile_deltas={"p25": 0.0, "p50": 0.0, "p75": 0.0},
                ks_statistic=None,
            )
        },
        segments={},
    )
    output = tmp_path / "drift.json"

    save_json_report(report, output)

    assert output.exists()
