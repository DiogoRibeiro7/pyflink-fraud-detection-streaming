from __future__ import annotations

import json
from pathlib import Path

import pytest

from fraud_streaming.aws_validation import (
    build_validation_report,
    parse_env_file,
    render_markdown_report,
)


def test_parse_env_file_ignores_comments_and_blank_lines(tmp_path: Path) -> None:
    env_path = tmp_path / "demo.env"
    env_path.write_text(
        "# comment\n\nAWS_REGION=eu-west-1\nTRANSACTIONS_TOPIC=transactions\n",
        encoding="utf-8",
    )

    values = parse_env_file(env_path)

    assert values["AWS_REGION"] == "eu-west-1"
    assert values["TRANSACTIONS_TOPIC"] == "transactions"


def test_build_validation_report_flags_placeholders_for_msk_mode() -> None:
    report = build_validation_report(
        mode="msk",
        mode_values={
            "AWS_REGION": "eu-west-1",
            "MSK_CLUSTER_ARN": "arn:aws:kafka:eu-west-1:123456789012:cluster/example/replace-me",
            "MSK_BOOTSTRAP_SERVERS": "replace-me:9098",
            "TRANSACTIONS_TOPIC": "transactions",
            "FRAUD_ALERTS_TOPIC": "fraud-alerts",
            "FLINK_CHECKPOINT_S3_URI": "s3://replace-me/flink/checkpoints/",
            "FLINK_LOG_GROUP": "/aws/kinesis-analytics/demo",
        },
        flink_values={"SOURCE_KIND": "msk", "SINK_KIND": "msk"},
    )

    assert report.ready_for_manual_validation is False
    assert any(finding.field_name == "MSK_CLUSTER_ARN" for finding in report.findings)


def test_build_validation_report_can_be_ready_when_values_are_realistic() -> None:
    report = build_validation_report(
        mode="kinesis",
        mode_values={
            "AWS_REGION": "eu-west-1",
            "TRANSACTIONS_STREAM": "fraud-transactions-dev",
            "FRAUD_ALERTS_STREAM": "fraud-alerts-dev",
            "FLINK_APP_NAME": "pyflink-fraud-streaming-dev",
            "FLINK_CODE_S3_URI": "s3://fraud-artifacts-dev/flink/app.zip",
            "FLINK_CHECKPOINT_S3_URI": "s3://fraud-state-dev/checkpoints/",
            "FLINK_LOG_GROUP": "/aws/kinesis-analytics/fraud-dev",
            "PRIVATE_SUBNET_IDS": "subnet-1a2b3c,subnet-4d5e6f",
            "SECURITY_GROUP_IDS": "sg-1234567",
        },
        flink_values={"SOURCE_KIND": "kinesis", "SINK_KIND": "kinesis"},
    )

    assert report.ready_for_manual_validation is True
    assert report.findings == []


def test_render_markdown_report_includes_mode_and_findings() -> None:
    report = build_validation_report(
        mode="msk",
        mode_values={},
        flink_values={},
    )

    markdown = render_markdown_report(report)

    assert "AWS Managed Validation Report" in markdown
    assert "`msk`" in markdown
    assert "required value is missing" in markdown
