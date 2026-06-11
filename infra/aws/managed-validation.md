# Managed AWS Validation Runbook

This runbook is the final manual step for the AWS templates already present in [`infra/aws/`](README.md).

It does not claim that the repository has already been validated in a real AWS account from this agent environment. What it does provide is a concrete, reproducible checklist for the real validation pass.

## Validation scope

Target path:

- producer writes transaction events
- managed streaming runtime executes the fraud pipeline
- alerts land in a downstream stream or storage target
- operational evidence is collected

Suggested paths:

1. MSK -> Managed Service for Apache Flink -> MSK alerts
2. MSK -> Managed Service for Apache Flink -> S3 / Iceberg
3. Kinesis -> Managed Service for Apache Flink -> Kinesis or S3

## Required evidence

Capture these artifacts during the real AWS validation run:

- application version or deployed artifact digest
- AWS region and environment name
- input stream name or ARN
- output stream, topic, or table identifiers
- at least one input transaction sample
- at least one emitted alert sample
- CloudWatch logs showing successful startup
- checkpoint or recovery evidence where available
- IAM role names used by the runtime

## Pre-flight checklist

- confirm all placeholder values in `infra/aws/env/*.example` were replaced
- confirm secrets are sourced from AWS Secrets Manager, Parameter Store, or another approved secret store
- confirm VPC, subnet, and security-group rules allow the chosen transport path
- confirm encryption is enabled for MSK, Kinesis, S3, and Glue-facing storage
- confirm connector and dependency JAR requirements for the deployed Flink runtime
- confirm the PyFlink artifact version matches the target managed runtime

Before any real deployment, run the repository's AWS validation helper against your adapted env files:

```bash
poetry run fraud-aws-validate \
  --mode msk \
  --env-file infra/aws/env/msk.env \
  --flink-env-file infra/aws/env/flink-app.env \
  --output artifacts/aws_validation_report.json \
  --markdown-output artifacts/aws_validation_report.md
```

Use `--mode kinesis` with the Kinesis env file for the Kinesis path. The command checks for:

- missing required keys
- placeholder values that were never replaced
- source-kind mismatches between the mode-specific env file and the Flink app env file

It does not talk to AWS. Its job is to catch obvious template mistakes before you start the real managed-cloud validation run.

## Smoke validation flow

1. Provision the chosen AWS template inputs.
2. Deploy the streaming application artifact.
3. Produce a bounded batch of known transaction events.
4. Verify the runtime accepts the source connection.
5. Verify at least one suspicious event produces an alert.
6. Verify benign traffic does not produce only high-risk noise.
7. Verify checkpointing or restart behavior if enabled.
8. Capture logs, outputs, and configuration hashes for the final report.

## Suggested final report sections

- deployment summary
- environment and runtime versions
- source and sink configuration
- validation results
- observed limitations
- follow-up fixes

## Honest status

From this repository alone, the AWS examples are implementation-ready templates plus this validation runbook.

A real end-to-end managed-cloud validation still requires:

- an AWS account
- networked managed services
- runtime artifact deployment
- manual evidence capture

Do not mark the managed-cloud validation item complete until that external execution has actually happened.
