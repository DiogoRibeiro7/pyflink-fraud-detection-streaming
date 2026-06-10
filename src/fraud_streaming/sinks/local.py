"""Local sink abstractions for transactions and alerts."""

from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, TextIO

from fraud_streaming.schemas import Alert, Transaction
from fraud_streaming.serialization import alert_to_json, transaction_to_json

AlertSinkKind = Literal["stdout", "jsonl", "parquet", "iceberg"]
TransactionSinkKind = Literal["none", "jsonl", "parquet", "iceberg"]


class AlertSink(Protocol):
    """Typed output interface for alert records."""

    def write(self, alert: Alert) -> None:
        """Write one alert record."""

    def close(self) -> None:
        """Flush and close sink resources."""


class TransactionSink(Protocol):
    """Typed output interface for transaction records."""

    def write(self, transaction: Transaction) -> None:
        """Write one transaction record."""

    def close(self) -> None:
        """Flush and close sink resources."""


class SupportsLocalSinkArgs(Protocol):
    """Minimal protocol for CLI sink arguments."""

    alert_sink: AlertSinkKind
    alert_output: Path | None
    transaction_sink: TransactionSinkKind
    transaction_output: Path | None
    iceberg_catalog_uri: str | None
    iceberg_warehouse: str | None
    iceberg_alert_table: str | None
    iceberg_transaction_table: str | None


@dataclass(frozen=True, slots=True)
class LocalSinkConfig:
    """Validated sink configuration for the local CLI."""

    alert_sink: AlertSinkKind
    alert_output: Path | None
    transaction_sink: TransactionSinkKind
    transaction_output: Path | None
    iceberg_catalog_uri: str | None
    iceberg_warehouse: str | None
    iceberg_alert_table: str | None
    iceberg_transaction_table: str | None


class StdoutAlertSink:
    """Write alerts as JSON lines to stdout."""

    def __init__(self, handle: TextIO | None = None) -> None:
        self._handle = sys.stdout if handle is None else handle

    def write(self, alert: Alert) -> None:
        self._handle.write(alert_to_json(alert) + "\n")

    def close(self) -> None:
        self._handle.flush()


class NullTransactionSink:
    """Drop transaction records when no sink is requested."""

    def write(self, transaction: Transaction) -> None:
        del transaction

    def close(self) -> None:
        return None


class JsonlAlertSink:
    """Write alerts to a JSONL file."""

    def __init__(self, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = output_path.open("w", encoding="utf-8")

    def write(self, alert: Alert) -> None:
        self._handle.write(alert_to_json(alert) + "\n")

    def close(self) -> None:
        self._handle.close()


class JsonlTransactionSink:
    """Write validated transactions to a JSONL file."""

    def __init__(self, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = output_path.open("w", encoding="utf-8")

    def write(self, transaction: Transaction) -> None:
        self._handle.write(transaction_to_json(transaction) + "\n")

    def close(self) -> None:
        self._handle.close()


class _ParquetSinkBase:
    """Buffer rows and write them as a Parquet file on close."""

    def __init__(self, output_path: Path) -> None:
        self._output_path = output_path
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        self._rows: list[dict[str, Any]] = []
        self._write_table = _require_parquet_writer()

    def _append_row(self, row: dict[str, Any]) -> None:
        self._rows.append(row)

    def close(self) -> None:
        self._write_table(self._rows, self._output_path)


class ParquetAlertSink(_ParquetSinkBase):
    """Buffer alerts and write them to Parquet."""

    def write(self, alert: Alert) -> None:
        self._append_row(alert.to_dict())


class ParquetTransactionSink(_ParquetSinkBase):
    """Buffer transactions and write them to Parquet."""

    def write(self, transaction: Transaction) -> None:
        self._append_row(transaction.to_dict())


def _require_parquet_writer() -> Callable[[list[dict[str, Any]], Path], None]:
    """Return a pyarrow-backed Parquet writer or raise a clear error."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "Parquet sinks require optional pyarrow support. Install it with `pip install pyarrow`."
        ) from exc

    def _write(rows: list[dict[str, Any]], output_path: Path) -> None:
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, output_path)

    return _write


def validate_local_sink_args(args: SupportsLocalSinkArgs) -> LocalSinkConfig:
    """Validate local sink CLI arguments before opening any files."""
    alert_output = args.alert_output
    transaction_output = args.transaction_output
    if args.alert_sink != "stdout" and alert_output is None:
        raise ValueError("--alert-output is required when --alert-sink is not stdout")
    if args.transaction_sink != "none" and transaction_output is None:
        raise ValueError("--transaction-output is required when --transaction-sink is not none")

    if alert_output is not None and alert_output.exists() and alert_output.is_dir():
        raise ValueError(f"alert output path is a directory: {alert_output}")
    if (
        transaction_output is not None
        and transaction_output.exists()
        and transaction_output.is_dir()
    ):
        raise ValueError(f"transaction output path is a directory: {transaction_output}")

    iceberg_selected = args.alert_sink == "iceberg" or args.transaction_sink == "iceberg"
    if iceberg_selected:
        if not args.iceberg_catalog_uri:
            raise ValueError("--iceberg-catalog-uri is required for iceberg sinks")
        if not args.iceberg_warehouse:
            raise ValueError("--iceberg-warehouse is required for iceberg sinks")
        if args.alert_sink == "iceberg" and not args.iceberg_alert_table:
            raise ValueError("--iceberg-alert-table is required when --alert-sink=iceberg")
        if args.transaction_sink == "iceberg" and not args.iceberg_transaction_table:
            raise ValueError(
                "--iceberg-transaction-table is required when --transaction-sink=iceberg"
            )

    return LocalSinkConfig(
        alert_sink=args.alert_sink,
        alert_output=alert_output,
        transaction_sink=args.transaction_sink,
        transaction_output=transaction_output,
        iceberg_catalog_uri=args.iceberg_catalog_uri,
        iceberg_warehouse=args.iceberg_warehouse,
        iceberg_alert_table=args.iceberg_alert_table,
        iceberg_transaction_table=args.iceberg_transaction_table,
    )
