"""Sink abstractions and local sink factories."""

from __future__ import annotations

from fraud_streaming.sinks.iceberg import (
    IcebergAlertSink,
    IcebergSinkConfig,
    IcebergTransactionSink,
)
from fraud_streaming.sinks.local import (
    AlertSink,
    JsonlAlertSink,
    JsonlTransactionSink,
    LocalSinkConfig,
    NullTransactionSink,
    ParquetAlertSink,
    ParquetTransactionSink,
    StdoutAlertSink,
    TransactionSink,
    validate_local_sink_args,
)

__all__ = [
    "AlertSink",
    "IcebergAlertSink",
    "IcebergSinkConfig",
    "IcebergTransactionSink",
    "JsonlAlertSink",
    "JsonlTransactionSink",
    "LocalSinkConfig",
    "NullTransactionSink",
    "ParquetAlertSink",
    "ParquetTransactionSink",
    "StdoutAlertSink",
    "TransactionSink",
    "validate_local_sink_args",
]
