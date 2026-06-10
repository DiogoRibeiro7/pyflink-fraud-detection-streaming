from __future__ import annotations

import pickle
from datetime import datetime, timezone
from pathlib import Path

import pytest

from fraud_streaming.local_runner import process_transaction
from fraud_streaming.ml.scoring import ModelScorer, ScoringConfig, combine_scores
from fraud_streaming.rules import RuleScore
from fraud_streaming.schemas import Transaction


class FakeVectorizer:
    def transform(self, X: list[dict[str, float | str]]) -> list[dict[str, float | str]]:
        return X


class FakeModel:
    def __init__(self, probability: float) -> None:
        self._probability = probability

    def predict_proba(self, X: list[dict[str, float | str]]) -> list[list[float]]:
        return [[1.0 - self._probability, self._probability] for _ in X]


def make_transaction() -> Transaction:
    return Transaction(
        transaction_id="tx-1",
        user_id="user-1",
        card_id="card-1",
        merchant_id="merchant-1",
        amount=950.0,
        currency="EUR",
        country="US",
        device_id="device-2",
        merchant_category="electronics",
        event_time=datetime(2026, 6, 10, 23, 0, tzinfo=timezone.utc),
        channel="online",
        is_card_present=False,
    )


def write_model_artifact(
    path: Path,
    *,
    probability: float,
    raw_feature_schema: tuple[str, ...],
) -> None:
    bundle = {
        "model": FakeModel(probability),
        "vectorizer": FakeVectorizer(),
        "raw_feature_schema": list(raw_feature_schema),
        "vectorized_feature_names": list(raw_feature_schema),
    }
    with path.open("wb") as handle:
        pickle.dump(bundle, handle)


def test_model_scorer_loads_artifact_and_scores(tmp_path: Path) -> None:
    artifact_path = tmp_path / "model.pkl"
    from fraud_streaming.ml.training import CANONICAL_FEATURE_SCHEMA

    write_model_artifact(
        artifact_path,
        probability=0.8,
        raw_feature_schema=CANONICAL_FEATURE_SCHEMA,
    )

    scorer = ModelScorer.from_artifact(artifact_path)
    score = scorer.score(
        {name: 0.0 for name in CANONICAL_FEATURE_SCHEMA[:-4]}
        | {
            "country": "PT",
            "merchant_category": "grocery",
            "channel": "pos",
            "is_card_present": 1.0,
        }
    )

    assert score == 0.8


def test_model_scorer_rejects_schema_mismatch(tmp_path: Path) -> None:
    artifact_path = tmp_path / "model.pkl"
    write_model_artifact(artifact_path, probability=0.5, raw_feature_schema=("amount",))

    with pytest.raises(ValueError, match="raw_feature_schema"):
        ModelScorer.from_artifact(artifact_path)


def test_combine_scores_supports_rules_model_and_blend() -> None:
    rule_score = RuleScore(risk_score=40, risk_level="medium", reasons=["rule reason"])

    rules_only = combine_scores(rule_score=rule_score, model_score=None, strategy="rules")
    model_only = combine_scores(rule_score=rule_score, model_score=0.8, strategy="model")
    blended = combine_scores(
        rule_score=rule_score,
        model_score=0.8,
        strategy="blend",
        rule_weight=0.25,
        model_weight=0.75,
    )

    assert rules_only.risk_score == 40
    assert model_only.risk_score == 80
    assert blended.risk_score == 70
    assert "ml model score=0.800" in model_only.reasons


def test_process_transaction_uses_model_scorer_when_requested(tmp_path: Path) -> None:
    artifact_path = tmp_path / "model.pkl"
    from fraud_streaming.ml.training import CANONICAL_FEATURE_SCHEMA

    write_model_artifact(
        artifact_path,
        probability=0.9,
        raw_feature_schema=CANONICAL_FEATURE_SCHEMA,
    )
    scorer = ModelScorer.from_artifact(artifact_path)
    alert = process_transaction(
        make_transaction(),
        {},
        scoring_config=ScoringConfig(strategy="model", model_artifact_path=artifact_path),
        model_scorer=scorer,
    )

    assert alert.risk_score == 90
    assert "ml model score=0.900" in alert.reasons


def test_process_transaction_defaults_to_rules_only() -> None:
    alert = process_transaction(make_transaction(), {})

    assert alert.risk_score >= 0
    assert all("ml model score=" not in reason for reason in alert.reasons)
