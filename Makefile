PYTHON ?= python

.PHONY: install test lint typecheck quality local-demo demo format generate benchmark compose-up compose-down produce-demo consume-alerts run-flink-kafka-demo clean

install:
	poetry install --with dev

test:
	poetry run pytest

lint:
	poetry run ruff check src tests scripts

format:
	poetry run ruff format --check src tests scripts

typecheck:
	poetry run mypy src

quality: format lint typecheck test

local-demo:
	poetry run python -c "import sys; sys.path.insert(0, 'src'); from fraud_streaming.cli import main; raise SystemExit(main(['data/sample_transactions.jsonl', '--show-all']))"

demo: local-demo

generate:
	$(PYTHON) scripts/generate_transactions.py --output data/generated_transactions.jsonl --users 20 --transactions 500 --seed 7

benchmark:
	poetry run python benchmarks/benchmark_local_runner.py --users 10 --transactions 1000 --repeats 3 --output artifacts/benchmarks/local_runner_benchmark.json --markdown-output artifacts/benchmarks/local_runner_benchmark.md

compose-up:
	docker compose --profile streaming-demo up --build -d redpanda topic-init flink-jobmanager flink-taskmanager

compose-down:
	docker compose --profile streaming-demo down --remove-orphans

produce-demo:
	docker compose --profile streaming-demo run --rm fraud-producer

consume-alerts:
	docker compose --profile streaming-demo run --rm fraud-consumer

run-flink-kafka-demo:
	docker compose --profile streaming-demo run --rm flink-submit

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache dist build *.egg-info
