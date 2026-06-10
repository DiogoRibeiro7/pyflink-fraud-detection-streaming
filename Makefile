PYTHON ?= python

.PHONY: install test lint typecheck demo generate clean

install:
	poetry install --with dev

test:
	poetry run pytest

lint:
	poetry run ruff check src tests scripts

typecheck:
	poetry run mypy src

demo:
	poetry run fraud-local data/sample_transactions.jsonl --show-all

generate:
	$(PYTHON) scripts/generate_transactions.py --output data/generated_transactions.jsonl --users 20 --transactions 500 --seed 7

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache dist build *.egg-info
