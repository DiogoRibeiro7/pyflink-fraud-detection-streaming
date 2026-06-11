from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from fraud_streaming.features import compute_features
from fraud_streaming.local_runner import process_json_lines
from fraud_streaming.rules import build_alert, score_features
from fraud_streaming.schemas import Alert, Transaction, UserProfileState
from fraud_streaming.sinks.iceberg import (
    IcebergAlertSink,
    IcebergSinkConfig,
    IcebergTransactionSink,
)
from fraud_streaming.sinks.local import (
    JsonlAlertSink,
    JsonlTransactionSink,
    validate_local_sink_args,
)


def make_transaction(transaction_id: str = "tx-1", amount: float = 42.0) -> Transaction:
    return Transaction(
        transaction_id=transaction_id,
        user_id="user-1",
        card_id="card-1",
        merchant_id="merchant-1",
        amount=amount,
        currency="EUR",
        country="PT",
        device_id="device-1",
        merchant_category="grocery",
        event_time=datetime(2026, 6, 10, 12, 0, tzinfo=timezone.utc),
        channel="pos",
        is_card_present=True,
    )


def make_alert() -> Alert:
    transaction = make_transaction()
    features = compute_features(transaction, UserProfileState())
    return build_alert(features, score_features(features))


def make_args(**overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "alert_sink": "stdout",
        "alert_output": None,
        "transaction_sink": "none",
        "transaction_output": None,
        "iceberg_catalog_uri": None,
        "iceberg_warehouse": None,
        "iceberg_alert_table": None,
        "iceberg_transaction_table": None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_validate_local_sink_args_accepts_default_stdout_configuration() -> None:
    config = validate_local_sink_args(make_args())

    assert config.alert_sink == "stdout"
    assert config.transaction_sink == "none"


def test_validate_local_sink_args_requires_output_paths_for_file_sinks() -> None:
    with pytest.raises(ValueError, match="--alert-output is required"):
        validate_local_sink_args(make_args(alert_sink="jsonl"))

    with pytest.raises(ValueError, match="--transaction-output is required"):
        validate_local_sink_args(make_args(transaction_sink="jsonl"))


def test_validate_local_sink_args_requires_iceberg_configuration() -> None:
    with pytest.raises(ValueError, match="--iceberg-catalog-uri is required"):
        validate_local_sink_args(make_args(alert_sink="iceberg", alert_output=Path("ignored")))


def test_validate_local_sink_args_allows_iceberg_without_file_output_paths() -> None:
    config = validate_local_sink_args(
        make_args(
            alert_sink="iceberg",
            transaction_sink="iceberg",
            iceberg_catalog_uri="sqlite:///tmp/catalog.db",
            iceberg_warehouse="tmp/warehouse",
            iceberg_alert_table="fraud.alerts",
            iceberg_transaction_table="fraud.transactions",
        )
    )

    assert config.alert_output is None
    assert config.transaction_output is None


def test_jsonl_alert_sink_writes_canonical_alert_json(tmp_path: Path) -> None:
    output = tmp_path / "alerts.jsonl"
    sink = JsonlAlertSink(output)

    sink.write(make_alert())
    sink.close()

    lines = output.read_text(encoding="utf-8").strip().splitlines()
    payload = json.loads(lines[0])
    assert payload["transaction_id"] == "tx-1"
    assert payload["features"]["amount"] == 42.0


def test_jsonl_transaction_sink_writes_canonical_transaction_json(tmp_path: Path) -> None:
    output = tmp_path / "transactions.jsonl"
    sink = JsonlTransactionSink(output)

    sink.write(make_transaction())
    sink.close()

    lines = output.read_text(encoding="utf-8").strip().splitlines()
    payload = json.loads(lines[0])
    assert payload["transaction_id"] == "tx-1"
    assert payload["amount"] == 42.0


def test_process_json_lines_can_write_transaction_and_alert_sinks(tmp_path: Path) -> None:
    alert_output = tmp_path / "alerts.jsonl"
    transaction_output = tmp_path / "transactions.jsonl"
    alert_sink = JsonlAlertSink(alert_output)
    transaction_sink = JsonlTransactionSink(transaction_output)
    line = json.dumps(make_transaction().to_dict())

    alerts = list(
        process_json_lines(
            [line],
            emit_low_risk=True,
            transaction_sink=transaction_sink,
            alert_sink=alert_sink,
        )
    )
    alert_sink.close()
    transaction_sink.close()

    assert len(alerts) == 1
    assert '"transaction_id":"tx-1"' in alert_output.read_text(encoding="utf-8")
    assert '"transaction_id":"tx-1"' in transaction_output.read_text(encoding="utf-8")


class FakeNoSuchTableError(Exception):
    pass


class FakeNamespaceAlreadyExistsError(Exception):
    pass


class FakeTableAlreadyExistsError(Exception):
    pass


class FakeArrowTable:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.schema = {"columns": tuple(rows[0].keys()) if rows else ()}


class FakePyArrow:
    class Table:
        @staticmethod
        def from_pylist(rows: list[dict[str, object]]) -> FakeArrowTable:
            return FakeArrowTable(rows)


class FakeIcebergTable:
    def __init__(self, schema: object | None = None) -> None:
        self.schema = schema
        self.append_calls: list[tuple[FakeArrowTable, dict[str, str]]] = []

    def append(
        self, rows_table: FakeArrowTable, snapshot_properties: dict[str, str] | None = None
    ) -> None:
        self.append_calls.append(
            (FakeArrowTable(list(rows_table.rows)), dict(snapshot_properties or {}))
        )


class FakeCatalog:
    def __init__(self, existing_table: FakeIcebergTable | None = None) -> None:
        self.existing_table = existing_table
        self.created_namespaces: list[str] = []
        self.created_tables: list[tuple[str, object, dict[str, str]]] = []

    def load_table(self, table_name: str) -> FakeIcebergTable:
        if self.existing_table is None:
            raise FakeNoSuchTableError(table_name)
        return self.existing_table

    def create_namespace(self, namespace: str) -> None:
        self.created_namespaces.append(namespace)

    def create_table(
        self, table_name: str, schema: object, properties: dict[str, str]
    ) -> FakeIcebergTable:
        table = FakeIcebergTable(schema=schema)
        self.created_tables.append((table_name, schema, properties))
        self.existing_table = table
        return table


class FakeRuntime:
    def __init__(self, catalog: FakeCatalog) -> None:
        self.catalog = catalog
        self.pa = FakePyArrow()
        self.NoSuchTableError = FakeNoSuchTableError
        self.NamespaceAlreadyExistsError = FakeNamespaceAlreadyExistsError
        self.TableAlreadyExistsError = FakeTableAlreadyExistsError
        self.catalog_loads: list[tuple[str, dict[str, str]]] = []

    def load_catalog(self, name: str, **properties: str) -> FakeCatalog:
        self.catalog_loads.append((name, properties))
        return self.catalog


def test_iceberg_alert_sink_creates_table_and_appends_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = FakeRuntime(FakeCatalog())
    monkeypatch.setattr("fraud_streaming.sinks.iceberg._load_iceberg_runtime", lambda: runtime)

    sink = IcebergAlertSink(
        IcebergSinkConfig(
            catalog_uri="sqlite:///tmp/catalog.db",
            warehouse="artifacts/warehouse",
            table_name="fraud.alerts",
        )
    )
    sink.write(make_alert())
    sink.close()

    assert runtime.catalog_loads[0][0] == "fraud_streaming"
    assert runtime.catalog_loads[0][1]["type"] == "sql"
    assert runtime.catalog.created_namespaces == ["fraud"]
    assert runtime.catalog.created_tables[0][0] == "fraud.alerts"
    table = runtime.catalog.existing_table
    assert table is not None
    assert len(table.append_calls) == 1
    rows_table, snapshot_properties = table.append_calls[0]
    assert rows_table.rows[0]["transaction_id"] == "tx-1"
    assert snapshot_properties["fraud_streaming.record_count"] == "1"


def test_iceberg_transaction_sink_appends_to_existing_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing_table = FakeIcebergTable()
    runtime = FakeRuntime(FakeCatalog(existing_table=existing_table))
    monkeypatch.setattr("fraud_streaming.sinks.iceberg._load_iceberg_runtime", lambda: runtime)

    sink = IcebergTransactionSink(
        IcebergSinkConfig(
            catalog_uri="https://catalog.example.test",
            warehouse="s3://fraud-warehouse/demo",
            table_name="fraud.transactions",
        )
    )
    sink.write(make_transaction())
    sink.close()

    assert runtime.catalog_loads[0][1]["type"] == "rest"
    assert runtime.catalog.created_tables == []
    assert len(existing_table.append_calls) == 1
    rows_table, _snapshot_properties = existing_table.append_calls[0]
    assert rows_table.rows[0]["amount"] == 42.0
