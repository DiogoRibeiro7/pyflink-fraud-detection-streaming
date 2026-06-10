from __future__ import annotations

from pathlib import Path

from fraud_streaming.ml.feature_parity import (
    build_feature_rows_from_jsonl,
    compare_feature_datasets,
    save_report,
)


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    import json

    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_build_feature_rows_from_jsonl_is_deterministic(tmp_path: Path) -> None:
    rows = [
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
            "user_id": "user-1",
            "card_id": "card-1",
            "merchant_id": "merchant-2",
            "amount": 25.0,
            "currency": "EUR",
            "country": "US",
            "device_id": "device-2",
            "merchant_category": "travel",
            "event_time": "2026-06-10T12:01:00Z",
            "channel": "online",
            "is_card_present": False,
        },
    ]
    path = tmp_path / "input.jsonl"
    write_jsonl(path, rows)

    first = build_feature_rows_from_jsonl(path)
    second = build_feature_rows_from_jsonl(path)

    assert first == second
    assert first[1].feature_values["tx_count_5m"] == 2.0


def test_compare_feature_datasets_passes_for_identical_rows(tmp_path: Path) -> None:
    rows = [
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
        }
    ]
    reference = tmp_path / "reference.jsonl"
    current = tmp_path / "current.jsonl"
    write_jsonl(reference, rows)
    write_jsonl(current, rows)

    report = compare_feature_datasets(
        build_feature_rows_from_jsonl(reference),
        build_feature_rows_from_jsonl(current),
        deterministic=True,
    )

    assert report.passed is True


def test_compare_feature_datasets_detects_value_drift(tmp_path: Path) -> None:
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
            }
        ],
    )
    write_jsonl(
        current,
        [
            {
                "transaction_id": "tx-1",
                "user_id": "user-1",
                "card_id": "card-1",
                "merchant_id": "merchant-1",
                "amount": 999.0,
                "currency": "EUR",
                "country": "PT",
                "device_id": "device-1",
                "merchant_category": "grocery",
                "event_time": "2026-06-10T12:00:00Z",
                "channel": "pos",
                "is_card_present": True,
            }
        ],
    )

    report = compare_feature_datasets(
        build_feature_rows_from_jsonl(reference),
        build_feature_rows_from_jsonl(current),
        deterministic=True,
    )

    assert report.passed is False
    assert any(
        check.check_name == "deterministic_values" and not check.passed for check in report.checks
    )


def test_save_report_writes_json(tmp_path: Path) -> None:
    output = tmp_path / "parity.json"
    report = compare_feature_datasets([], [], deterministic=False)

    save_report(report, output)

    assert output.exists()
