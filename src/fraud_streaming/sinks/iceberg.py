"""Optional PyIceberg-backed sinks for alerts and validated transactions."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

from fraud_streaming.schemas import Alert, Transaction


@dataclass(frozen=True, slots=True)
class IcebergSinkConfig:
    """Configuration needed for an Iceberg-backed sink."""

    catalog_uri: str
    warehouse: str
    table_name: str


class _ArrowTable(Protocol):
    """Minimal Arrow table interface used by the sink."""

    schema: object


class _IcebergTable(Protocol):
    """Minimal Iceberg table interface used by the sink."""

    def append(self, rows_table: _ArrowTable, snapshot_properties: dict[str, str]) -> None:
        """Append an Arrow table to the Iceberg table."""


class _Catalog(Protocol):
    """Minimal PyIceberg catalog interface used by the sink."""

    def load_table(self, table_name: str) -> _IcebergTable:
        """Load an existing Iceberg table."""

    def create_namespace(self, namespace: str) -> None:
        """Create a namespace when it does not yet exist."""

    def create_table(
        self, table_name: str, schema: object, properties: dict[str, str]
    ) -> _IcebergTable:
        """Create a new Iceberg table."""


@dataclass(frozen=True, slots=True)
class _LoadedIcebergRuntime:
    """Concrete runtime bundle for PyIceberg and PyArrow imports."""

    pa: Any
    load_catalog: Callable[..., _Catalog]
    NoSuchTableError: type[Exception]
    NamespaceAlreadyExistsError: type[Exception]
    TableAlreadyExistsError: type[Exception]


def _load_iceberg_runtime() -> _LoadedIcebergRuntime:
    """Import optional PyIceberg dependencies or raise a clear error."""
    try:
        import pyarrow as pa
        from pyiceberg.catalog import load_catalog
        from pyiceberg.exceptions import (
            NamespaceAlreadyExistsError,
            NoSuchTableError,
            TableAlreadyExistsError,
        )
    except ImportError as exc:
        raise RuntimeError(
            "Iceberg sinks require optional Iceberg support. "
            "Install it with `poetry install -E iceberg` or `pip install pyiceberg pyarrow`."
        ) from exc
    return _LoadedIcebergRuntime(
        pa=pa,
        load_catalog=load_catalog,
        NoSuchTableError=NoSuchTableError,
        NamespaceAlreadyExistsError=NamespaceAlreadyExistsError,
        TableAlreadyExistsError=TableAlreadyExistsError,
    )


def _infer_catalog_type(catalog_uri: str) -> str:
    """Infer the PyIceberg catalog type from the configured URI."""
    parsed = urlparse(catalog_uri)
    scheme = parsed.scheme.lower()
    if scheme in {"http", "https"}:
        return "rest"
    if scheme in {"sqlite", "postgresql", "postgres", "mysql"}:
        return "sql"
    raise ValueError(
        "Could not infer Iceberg catalog type from --iceberg-catalog-uri. "
        "Use an HTTP(S) URI for a REST catalog or a SQLAlchemy-style database URI "
        "for a SQL catalog."
    )


def _normalize_warehouse(warehouse: str) -> str:
    """Convert local warehouse paths to file URIs while preserving remote URIs."""
    parsed = urlparse(warehouse)
    if parsed.scheme and len(parsed.scheme) > 1:
        return warehouse
    return Path(warehouse).resolve().as_uri()


def _split_table_identifier(identifier: str) -> tuple[str | None, str]:
    """Split a fully qualified Iceberg identifier into namespace and table name."""
    parts = [part.strip() for part in identifier.split(".") if part.strip()]
    if not parts:
        raise ValueError("Iceberg table name must not be empty")
    if len(parts) == 1:
        return None, parts[0]
    return ".".join(parts[:-1]), parts[-1]


def _catalog_properties(config: IcebergSinkConfig) -> dict[str, str]:
    """Build PyIceberg catalog properties from the sink configuration."""
    return {
        "type": _infer_catalog_type(config.catalog_uri),
        "uri": config.catalog_uri,
        "warehouse": _normalize_warehouse(config.warehouse),
    }


class _IcebergSinkBase:
    """Buffer rows and append them to an Iceberg table on close."""

    def __init__(self, config: IcebergSinkConfig) -> None:
        self._runtime = _load_iceberg_runtime()
        self._config = config
        self._catalog: _Catalog = self._runtime.load_catalog(
            "fraud_streaming", **_catalog_properties(config)
        )
        self._rows: list[dict[str, Any]] = []

    def _append(self, row: dict[str, Any]) -> None:
        self._rows.append(row)

    def _load_or_create_table(self, rows_table: _ArrowTable) -> _IcebergTable:
        namespace, _table_name = _split_table_identifier(self._config.table_name)
        try:
            return self._catalog.load_table(self._config.table_name)
        except self._runtime.NoSuchTableError:
            if namespace is not None:
                with suppress(self._runtime.NamespaceAlreadyExistsError):
                    self._catalog.create_namespace(namespace)
            try:
                return self._catalog.create_table(
                    self._config.table_name,
                    schema=rows_table.schema,
                    properties={"format-version": "2"},
                )
            except self._runtime.TableAlreadyExistsError:
                return self._catalog.load_table(self._config.table_name)

    def close(self) -> None:
        if not self._rows:
            return
        rows_table: _ArrowTable = self._runtime.pa.Table.from_pylist(self._rows)
        table = self._load_or_create_table(rows_table)
        try:
            table.append(
                rows_table,
                snapshot_properties={
                    "fraud_streaming.record_count": str(len(self._rows)),
                    "fraud_streaming.source": Path(__file__).name,
                },
            )
        finally:
            self._rows.clear()


class IcebergAlertSink(_IcebergSinkBase):
    """Iceberg sink for emitted fraud alerts."""

    def write(self, alert: Alert) -> None:
        self._append(alert.to_dict())


class IcebergTransactionSink(_IcebergSinkBase):
    """Iceberg sink for validated transactions."""

    def write(self, transaction: Transaction) -> None:
        self._append(transaction.to_dict())
