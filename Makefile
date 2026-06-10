PYTHON ?= python

.PHONY: install test lint typecheck quality local-demo demo format generate clean

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

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache dist build *.egg-info
