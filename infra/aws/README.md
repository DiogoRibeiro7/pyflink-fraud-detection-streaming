# AWS Deployment Examples

These examples are documentation-first templates for running this project on AWS. They are intentionally incomplete by design: they show the moving parts, security boundaries, and configuration surfaces you must adapt before any real deployment.

Nothing in this directory provisions a working production environment by itself.

## Scope

The examples cover:

- Amazon MSK as a Kafka-compatible source and sink
- Amazon Kinesis Data Streams as an alternative stream transport
- Amazon Managed Service for Apache Flink deployment considerations
- Amazon S3 plus AWS Glue Catalog for Iceberg-oriented output

## Recommended reading

- Amazon MSK getting started: https://docs.aws.amazon.com/msk/latest/developerguide/getting-started.html
- Amazon Managed Service for Apache Flink: https://docs.aws.amazon.com/managed-flink/
- Amazon Kinesis Data Streams: https://docs.aws.amazon.com/streams/latest/dev/introduction.html
- AWS Glue Iceberg support: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html
- AWS prescriptive guidance for Iceberg on AWS: https://docs.aws.amazon.com/prescriptive-guidance/latest/apache-iceberg-on-aws/introduction.html

## Deployment patterns

### 1. MSK + Managed Service for Apache Flink

Use this path when you want the closest managed analogue to the repository's Kafka-first architecture.

Typical shape:

- producers write transaction events to an Amazon MSK topic
- a Managed Service for Apache Flink application consumes transactions
- the Flink job emits alerts to another MSK topic or writes downstream data to S3/Iceberg
- analysts or downstream services consume alert events

Good fit:

- existing Kafka ecosystems
- teams already using Kafka clients, topics, and ACLs
- portfolio demos that want a clear mapping from local Redpanda/Kafka to AWS

### 2. Kinesis Data Streams + Managed Service for Apache Flink

Use this path when you want a more AWS-native streaming surface and do not need Kafka protocol compatibility.

Typical shape:

- producers write JSON transaction events to a Kinesis data stream
- a Managed Service for Apache Flink application reads from Kinesis
- the Flink job writes alerts to another Kinesis stream, Firehose, or S3
- downstream consumers inspect alerts or load them into analytical storage

Good fit:

- lower operational overhead when Kafka compatibility is not required
- AWS-native IAM-centered access control
- simpler event ingress for internal AWS workloads

### 3. S3 + Glue Catalog + Iceberg tables

Use this path when you want durable analytical storage for:

- raw validated transactions
- emitted fraud alerts
- analyst feedback and retraining exports

Typical shape:

- Flink or downstream batch jobs write data files to S3
- Glue Catalog stores Iceberg table metadata
- Athena, Glue, EMR, or other engines read the resulting tables

## Security notes

Treat these as minimum baseline concerns, not optional extras:

- IAM least privilege:
  separate roles for producers, Flink runtime, catalog access, and analysts
- VPC placement:
  keep MSK brokers, Managed Flink, and private consumers in controlled subnets
- secrets handling:
  use Secrets Manager or Parameter Store, not committed plaintext credentials
- encryption:
  enable encryption in transit and at rest for MSK, Kinesis, S3, and Glue-connected data paths
- logging and audit:
  wire CloudWatch logs, S3 access logs where relevant, and CloudTrail for control-plane actions

## Networking notes

Minimum network planning usually includes:

- private subnets for streaming infrastructure
- security groups that allow only the required broker or stream access paths
- NAT or VPC endpoints depending on how your runtime fetches dependencies and writes logs
- explicit DNS and connectivity checks for any self-managed dependencies you still keep

## Managed Service for Apache Flink notes

This repository's PyFlink wrapper is useful for local development and conceptual mapping, but Managed Service for Apache Flink deployment needs additional operational work:

- package code and dependencies into a deployable artifact
- externalize runtime properties such as topic names, stream names, and sink targets
- plan checkpoint, restart, and autoscaling settings explicitly
- verify connector compatibility for the target runtime
- decide whether Python support is operationally acceptable for the workload, or whether a Java/Scala translation is warranted for production

See `env/flink-app.env.example` for the kind of configuration surfaces you typically need.
Use [`managed-validation.md`](managed-validation.md) as the final smoke-test and evidence checklist before claiming a real AWS validation pass.

## Files in this directory

- `env/msk.env.example`: placeholder variables for an MSK-backed deployment
- `env/kinesis.env.example`: placeholder variables for a Kinesis-backed deployment
- `env/flink-app.env.example`: placeholder Managed Flink application properties
- `terraform/`: coherent but minimal Terraform skeleton showing network and naming variables

## Before deploying anything

Change at least these categories first:

1. AWS account and region values
2. VPC, subnet, and security group identifiers
3. bucket names, stream names, and topic names
4. IAM role ARNs and trust policies
5. encryption and log retention settings
6. scaling, checkpoint, and retention defaults

## What this directory does not claim

- no committed credentials
- no claim of one-click production deployment
- no implicit cost safety
- no guarantee that Python packaging for Managed Flink is complete for your target runtime

Treat everything here as a template to review, adapt, and test in your own AWS account.
