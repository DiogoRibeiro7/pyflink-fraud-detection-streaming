"""Experimental Iceberg sink extension points behind optional imports."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fraud_streaming.schemas import Alert, Transaction


@dataclass(frozen=True, slots=True)
class IcebergSinkConfig:
    """Configuration needed for an Iceberg-backed sink."""

    catalog_uri: str
    warehouse: str
    table_name: str


def _require_iceberg() -> None:
    """Raise a clear error when Iceberg support is not installed."""
    try:
        import pyiceberg  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "Iceberg sinks require optional Iceberg support. "
            "Install it with `pip install pyiceberg pyarrow`."
        ) from exc


class _IcebergSinkBase:
    """Documented extension point for future Iceberg append support."""

    def __init__(self, config: IcebergSinkConfig) -> None:
        _require_iceberg()
        self._config = config
        self._rows: list[dict[str, Any]] = []

    def _append(self, row: dict[str, Any]) -> None:
        self._rows.append(row)

    def close(self) -> None:
        if not self._rows:
            return
        raise RuntimeError(
            "Iceberg sink append is a documented extension point in this repository. "
            "Use local JSONL or Parquet sinks for runnable demos, or extend "
            f"{Path(__file__).name} with your target catalog append logic."
        )


class IcebergAlertSink(_IcebergSinkBase):
    """Experimental Iceberg sink for fraud alerts."""

    def write(self, alert: Alert) -> None:
        self._append(alert.to_dict())


class IcebergTransactionSink(_IcebergSinkBase):
    """Experimental Iceberg sink for validated transactions."""

    def write(self, transaction: Transaction) -> None:
        self._append(transaction.to_dict())
