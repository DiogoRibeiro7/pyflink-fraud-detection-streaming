"""AWS managed validation helpers for the repository's deployment templates."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

ValidationMode = Literal["msk", "kinesis"]

PLACEHOLDER_MARKERS: tuple[str, ...] = (
    "replace-me",
    "example",
    "123456789012",
    "subnet-aaaa",
    "sg-aaaa",
    "vpc-aaaa",
)

REQUIRED_KEYS_BY_MODE: dict[ValidationMode, tuple[str, ...]] = {
    "msk": (
        "AWS_REGION",
        "MSK_CLUSTER_ARN",
        "MSK_BOOTSTRAP_SERVERS",
        "TRANSACTIONS_TOPIC",
        "FRAUD_ALERTS_TOPIC",
        "FLINK_CHECKPOINT_S3_URI",
        "FLINK_LOG_GROUP",
    ),
    "kinesis": (
        "AWS_REGION",
        "TRANSACTIONS_STREAM",
        "FRAUD_ALERTS_STREAM",
        "FLINK_APP_NAME",
        "FLINK_CODE_S3_URI",
        "FLINK_CHECKPOINT_S3_URI",
        "FLINK_LOG_GROUP",
        "PRIVATE_SUBNET_IDS",
        "SECURITY_GROUP_IDS",
    ),
}


@dataclass(frozen=True, slots=True)
class ValidationFinding:
    """One validation finding for AWS managed deployment readiness."""

    severity: Literal["error", "warning"]
    message: str
    field_name: str | None = None

    def to_dict(self) -> dict[str, str]:
        """Return a JSON-compatible finding."""
        payload = {"severity": self.severity, "message": self.message}
        if self.field_name is not None:
            payload["field_name"] = self.field_name
        return payload


@dataclass(frozen=True, slots=True)
class AwsValidationReport:
    """Structured managed-validation report scaffold."""

    mode: ValidationMode
    ready_for_manual_validation: bool
    findings: list[ValidationFinding]
    env_values: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible report."""
        return {
            "mode": self.mode,
            "ready_for_manual_validation": self.ready_for_manual_validation,
            "findings": [finding.to_dict() for finding in self.findings],
            "env_values": self.env_values,
            "required_evidence": [
                "deployment artifact version",
                "source stream or topic identifier",
                "output stream, topic, or table identifier",
                "CloudWatch startup logs",
                "sample suspicious input event",
                "sample emitted alert",
                "checkpoint or recovery evidence",
            ],
        }


def build_parser() -> argparse.ArgumentParser:
    """Create the AWS managed validation parser."""
    parser = argparse.ArgumentParser(
        description="Validate AWS managed-demo env files and emit a validation report scaffold."
    )
    parser.add_argument(
        "--mode",
        choices=["msk", "kinesis"],
        required=True,
        help="Managed validation mode to check.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        required=True,
        help="Primary env file for the chosen mode.",
    )
    parser.add_argument(
        "--flink-env-file",
        type=Path,
        default=Path("infra/aws/env/flink-app.env.example"),
        help="Managed Flink application env file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="JSON output path for the validation report.",
    )
    parser.add_argument(
        "--markdown-output",
        type=Path,
        help="Optional Markdown output path for a human-readable report.",
    )
    return parser


def validate_args(args: argparse.Namespace) -> None:
    """Validate CLI arguments for AWS managed validation."""
    for path_name in ("env_file", "flink_env_file"):
        path = getattr(args, path_name)
        if not path.exists():
            raise ValueError(f"{path_name.replace('_', '-')} does not exist: {path}")
        if not path.is_file():
            raise ValueError(f"{path_name.replace('_', '-')} is not a file: {path}")
    if args.output.parent != Path():
        args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.markdown_output is not None and args.markdown_output.parent != Path():
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)


def parse_env_file(path: Path) -> dict[str, str]:
    """Parse a simple KEY=VALUE env file while ignoring comments and blank lines."""
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"invalid env line at {path}:{line_number}")
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _contains_placeholder(value: str) -> bool:
    lowered = value.lower()
    return any(marker in lowered for marker in PLACEHOLDER_MARKERS)


def build_validation_report(
    *,
    mode: ValidationMode,
    mode_values: dict[str, str],
    flink_values: dict[str, str],
) -> AwsValidationReport:
    """Build a validation report from AWS env-file values."""
    findings: list[ValidationFinding] = []
    merged_values = dict(mode_values)
    merged_values.update({f"FLINK::{key}": value for key, value in flink_values.items()})

    for key in REQUIRED_KEYS_BY_MODE[mode]:
        value = mode_values.get(key, "")
        if not value:
            findings.append(
                ValidationFinding(
                    severity="error",
                    field_name=key,
                    message=f"required value is missing for {mode} validation",
                )
            )
        elif _contains_placeholder(value):
            findings.append(
                ValidationFinding(
                    severity="error",
                    field_name=key,
                    message="placeholder value must be replaced before AWS validation",
                )
            )

    source_kind = flink_values.get("SOURCE_KIND", "")
    sink_kind = flink_values.get("SINK_KIND", "")
    if source_kind and mode == "msk" and source_kind != "msk":
        findings.append(
            ValidationFinding(
                severity="warning",
                field_name="SOURCE_KIND",
                message="Flink app env does not currently match MSK validation mode",
            )
        )
    if source_kind and mode == "kinesis" and source_kind != "kinesis":
        findings.append(
            ValidationFinding(
                severity="warning",
                field_name="SOURCE_KIND",
                message="Flink app env does not currently match Kinesis validation mode",
            )
        )
    if not sink_kind:
        findings.append(
            ValidationFinding(
                severity="warning",
                field_name="SINK_KIND",
                message="sink kind is unset in the Flink application env file",
            )
        )

    for key, value in flink_values.items():
        if value and _contains_placeholder(value):
            findings.append(
                ValidationFinding(
                    severity="warning",
                    field_name=key,
                    message="placeholder value remains in managed Flink application env file",
                )
            )

    ready = not any(finding.severity == "error" for finding in findings)
    return AwsValidationReport(
        mode=mode,
        ready_for_manual_validation=ready,
        findings=findings,
        env_values=merged_values,
    )


def render_markdown_report(report: AwsValidationReport) -> str:
    """Render a human-readable Markdown summary of the validation report."""
    lines = [
        "# AWS Managed Validation Report",
        "",
        f"- Mode: `{report.mode}`",
        f"- Ready for manual validation: `{str(report.ready_for_manual_validation).lower()}`",
        "",
        "## Findings",
    ]
    if not report.findings:
        lines.append(
            "- No findings. The env files are structurally ready for manual AWS validation."
        )
    else:
        for finding in report.findings:
            field_suffix = "" if finding.field_name is None else f" (`{finding.field_name}`)"
            lines.append(f"- `{finding.severity}`{field_suffix}: {finding.message}")
    lines.extend(
        [
            "",
            "## Required Evidence",
            "- deployment artifact version",
            "- source stream or topic identifier",
            "- output stream, topic, or table identifier",
            "- CloudWatch startup logs",
            "- sample suspicious input event",
            "- sample emitted alert",
            "- checkpoint or recovery evidence",
        ]
    )
    return "\n".join(lines) + "\n"


def save_report(report: AwsValidationReport, output: Path) -> None:
    """Persist the JSON validation report."""
    output.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    """Run the AWS managed validation CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        validate_args(args)
        mode_values = parse_env_file(args.env_file)
        flink_values = parse_env_file(args.flink_env_file)
        report = build_validation_report(
            mode=args.mode,
            mode_values=mode_values,
            flink_values=flink_values,
        )
        save_report(report, args.output)
        if args.markdown_output is not None:
            args.markdown_output.write_text(
                render_markdown_report(report),
                encoding="utf-8",
            )
    except ValueError as exc:
        parser.exit(status=2, message=f"{exc}\n")

    print(f"Saved AWS validation report to {args.output}")
    if args.markdown_output is not None:
        print(f"Saved AWS validation Markdown report to {args.markdown_output}")
    return 0 if report.ready_for_manual_validation else 1
