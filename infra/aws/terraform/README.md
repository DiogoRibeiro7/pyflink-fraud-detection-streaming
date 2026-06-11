# Terraform Skeleton

This Terraform folder is a starting point for naming, tagging, and network inputs. It is intentionally conservative and does not pretend to stand up a full streaming stack by itself.

Use it when you want a coherent place to centralize:

- environment naming
- region selection
- VPC and subnet inputs
- S3 bucket names
- stream or topic names
- standard tags

Before creating real resources, decide whether your deployment target is:

- MSK + Managed Service for Apache Flink
- Kinesis Data Streams + Managed Service for Apache Flink
- a mixed architecture with S3/Glue/Iceberg outputs

Then extend `main.tf` and `variables.tf` with the specific services you want to own in Terraform.
