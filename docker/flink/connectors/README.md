# Flink Connector JARs

Place the Kafka connector JAR that matches the Flink runtime version used by `docker/flink/Dockerfile` in this directory before running the full streaming demo.

Current runtime target:

- Flink image: `1.19.3-scala_2.12-java17`

What to add manually:

- a Kafka connector JAR compatible with Flink `1.19.x`

This repository does not vendor the connector binary in git. Download it from the official Apache Flink releases/downloads page and keep the version alignment explicit.
