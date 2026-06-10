# Contributing

## Local setup

Install the baseline development environment with Poetry:

```bash
poetry install --with dev
```

This installs the dependencies needed for local development, tests, linting, and type checking. PyFlink remains optional so the ordinary Python workflow stays lightweight.

## Quality checks

Run the full local quality gate before opening a pull request:

```bash
make quality
```

That target runs:

```bash
poetry run ruff format --check src tests scripts
poetry run ruff check src tests scripts
poetry run mypy src
poetry run pytest
```

You can also run the individual commands:

```bash
make test
make lint
make typecheck
```

## Local demo

Run the local fraud pipeline against the included sample data:

```bash
make local-demo
```

This does not require Kafka or Flink.

## Optional extras

Install PyFlink only when you need the streaming job wrapper:

```bash
poetry install --with dev -E flink
```

Then run:

```bash
poetry run pyflink-fraud-job --source file --input data/sample_transactions.jsonl --sink stdout
```

Kafka-related functionality should remain optional as the repository evolves. Ordinary tests and local development must continue to work without Kafka, Flink, Docker, or external services.

## Development expectations

- Keep fraud logic in pure Python modules such as `features.py`, `rules.py`, `schemas.py`, and `serialization.py`.
- Add tests for new pure-Python logic.
- Validate external input aggressively.
- Do not introduce required heavy dependencies for unit tests.
- Update documentation for user-facing changes.
