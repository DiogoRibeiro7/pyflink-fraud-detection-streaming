"""Optional model scoring and rule/model score combination."""

from __future__ import annotations

import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from fraud_streaming.ml.training import CANONICAL_FEATURE_SCHEMA, build_feature_dict
from fraud_streaming.rules import RuleScore, risk_level_from_score
from fraud_streaming.schemas import FraudFeatures, Transaction


class VectorizerLike(Protocol):
    """Minimal protocol for vectorizer objects stored in model bundles."""

    def transform(self, X: list[dict[str, float | str]]) -> Any:
        """Transform feature dictionaries into model-ready features."""


class ProbabilityModelLike(Protocol):
    """Minimal protocol for probability-like model objects."""

    def predict_proba(self, X: Any) -> Any:
        """Predict class probabilities for feature vectors."""


@dataclass(frozen=True, slots=True)
class ScoringConfig:
    """Configuration for combining rule-based and model-based scores."""

    strategy: str = "rules"
    model_artifact_path: Path | None = None
    rule_weight: float = 0.5
    model_weight: float = 0.5

    def __post_init__(self) -> None:
        valid_strategies = {"rules", "model", "blend"}
        if self.strategy not in valid_strategies:
            valid = ", ".join(sorted(valid_strategies))
            raise ValueError(f"strategy must be one of: {valid}")
        if self.rule_weight < 0 or self.model_weight < 0:
            raise ValueError("rule_weight and model_weight must be non-negative")
        if self.strategy in {"model", "blend"} and self.model_artifact_path is None:
            raise ValueError("model_artifact_path is required for model or blend strategy")
        if self.strategy == "blend" and self.rule_weight + self.model_weight <= 0:
            raise ValueError("rule_weight + model_weight must be positive for blend strategy")


@dataclass(frozen=True, slots=True)
class ModelBundle:
    """Loaded model artifact contents."""

    model: ProbabilityModelLike
    vectorizer: VectorizerLike
    raw_feature_schema: tuple[str, ...]
    vectorized_feature_names: list[str]


class ModelScorer:
    """Load and apply an optional offline-trained model artifact."""

    def __init__(
        self,
        *,
        model: ProbabilityModelLike,
        vectorizer: VectorizerLike,
        raw_feature_schema: tuple[str, ...],
        vectorized_feature_names: list[str],
    ) -> None:
        self._validate_feature_schema(raw_feature_schema)
        self._model = model
        self._vectorizer = vectorizer
        self._raw_feature_schema = raw_feature_schema
        self._vectorized_feature_names = vectorized_feature_names

    @classmethod
    def from_artifact(cls, artifact_path: Path) -> ModelScorer:
        """Load a scorer from a pickled model bundle."""
        if not artifact_path.exists():
            raise ValueError(f"model artifact does not exist: {artifact_path}")
        if not artifact_path.is_file():
            raise ValueError(f"model artifact path is not a file: {artifact_path}")

        with artifact_path.open("rb") as handle:
            payload = pickle.load(handle)
        if not isinstance(payload, dict):
            raise ValueError("model artifact must contain a dictionary bundle")

        try:
            model = payload["model"]
            vectorizer = payload["vectorizer"]
            raw_feature_schema = tuple(payload["raw_feature_schema"])
            vectorized_feature_names = list(payload["vectorized_feature_names"])
        except KeyError as exc:
            raise ValueError(f"model artifact is missing required key: {exc.args[0]}") from exc

        return cls(
            model=model,
            vectorizer=vectorizer,
            raw_feature_schema=raw_feature_schema,
            vectorized_feature_names=vectorized_feature_names,
        )

    @staticmethod
    def _validate_feature_schema(raw_feature_schema: tuple[str, ...]) -> None:
        """Validate the raw feature schema against the canonical training schema."""
        if raw_feature_schema != CANONICAL_FEATURE_SCHEMA:
            raise ValueError(
                "model artifact raw_feature_schema does not match the canonical feature schema"
            )

    @property
    def raw_feature_schema(self) -> tuple[str, ...]:
        """Return the raw feature schema used by the model."""
        return self._raw_feature_schema

    def score(self, feature_values: dict[str, float | str]) -> float:
        """Return a probability-like fraud score between 0 and 1."""
        missing = [name for name in self._raw_feature_schema if name not in feature_values]
        extra = [name for name in feature_values if name not in self._raw_feature_schema]
        if missing:
            raise ValueError(f"feature values are missing required fields: {', '.join(missing)}")
        if extra:
            raise ValueError(
                f"feature values contain unexpected fields: {', '.join(sorted(extra))}"
            )

        ordered = {name: feature_values[name] for name in self._raw_feature_schema}
        transformed = self._vectorizer.transform([ordered])
        probabilities = self._model.predict_proba(transformed)
        probability = float(probabilities[0][1])
        if not 0.0 <= probability <= 1.0:
            raise ValueError("model score must be between 0 and 1")
        return probability


def compute_model_score(
    scorer: ModelScorer,
    features: FraudFeatures,
    transaction: Transaction,
) -> float:
    """Build canonical feature values and score them with the model."""
    return scorer.score(build_feature_dict(features, transaction))


def combine_scores(
    *,
    rule_score: RuleScore,
    model_score: float | None,
    strategy: str,
    rule_weight: float = 0.5,
    model_weight: float = 0.5,
) -> RuleScore:
    """Combine rule-based and model-based scores while preserving explanations."""
    if strategy == "rules":
        return rule_score

    if model_score is None:
        raise ValueError("model_score is required for model or blend strategy")

    model_risk_score = round(model_score * 100)
    reasons = list(rule_score.reasons)
    reasons.append(f"ml model score={model_score:.3f}")

    if strategy == "model":
        return RuleScore(
            risk_score=model_risk_score,
            risk_level=risk_level_from_score(model_risk_score),
            reasons=reasons,
        )

    if strategy == "blend":
        combined = round(
            (rule_score.risk_score * rule_weight + model_risk_score * model_weight)
            / (rule_weight + model_weight)
        )
        return RuleScore(
            risk_score=combined,
            risk_level=risk_level_from_score(combined),
            reasons=reasons,
        )

    raise ValueError(f"unsupported scoring strategy: {strategy}")
