FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
COPY data ./data
COPY scripts ./scripts

RUN pip install --upgrade pip \
    && pip install -e . \
    && pip install "kafka-python>=2.0.2,<3.0.0"

ENTRYPOINT ["python", "-m", "fraud_streaming.cli"]
CMD ["data/sample_transactions.jsonl", "--show-all"]
