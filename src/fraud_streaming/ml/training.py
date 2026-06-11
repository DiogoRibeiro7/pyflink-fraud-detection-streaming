"""Offline baseline ML training for the fraud demo project."""

from __future__ import annotations

import argparse
import csv
import json
import pickle
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from fraud_streaming.config import DEFAULT_CONFIG, FraudConfig
from fraud_streaming.features import compute_features, update_state
from fraud_streaming.ml.dataset_mapping import apply_dataset_mapping, load_dataset_mapping
from fraud_streaming.rules import score_features
from fraud_streaming.schemas import FraudFeatures, Transaction, UserProfileState
from fraud_streaming.serialization import transaction_from_dict
from fraud_streaming.synthetic import generate_transactions

CANONICAL_FEATURE_SCHEMA: tuple[str, ...] = (
    "amount",
    "tx_count_5m",
    "amount_sum_1h",
    "amount_zscore",
    "minutes_since_last_tx",
    "country_changed",
    "device_changed",
    "card_not_present",
    "night_transaction",
    "high_velocity",
    "high_amount",
    "country",
    "merchant_category",
    "channel",
    "is_card_present",
)


class DictTransformer(Protocol):
    """Minimal protocol for feature transformers used in the bundle."""

    def fit_transform(self, X: list[dict[str, float | str]]) -> Any:
        """Fit and transform feature dictionaries."""

    def transform(self, X: list[dict[str, float | str]]) -> Any:
        """Transform feature dictionaries."""

    @property
    def vocabulary_(self) -> dict[str, int]:
        """Return the fitted feature vocabulary."""


class ClassifierLike(Protocol):
    """Minimal protocol for optional sklearn classifiers."""

    def fit(self, X: Any, y: list[int]) -> Any:
        """Fit the classifier."""

    def predict(self, X: Any) -> Any:
        """Predict class labels."""

    def predict_proba(self, X: Any) -> Any:
        """Predict class probabilities."""


@dataclass(frozen=True, slots=True)
class TrainingExample:
    """One offline training row before vectorization."""

    transaction_id: str
    event_time: str
    feature_values: dict[str, float | str]
    label: int
    label_source: str


@dataclass(frozen=True, slots=True)
class TrainingDataset:
    """Offline feature table and labels ready for training."""

    examples: list[TrainingExample]
    feature_schema: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TrainingArtifacts:
    """Paths for saved training artifacts."""

    run_dir: Path
    model_path: Path
    feature_schema_path: Path
    metrics_path: Path
    provenance_path: Path


@dataclass(frozen=True, slots=True)
class DatasetProvenance:
    """Documented provenance for a labelled or synthetic training dataset."""

    dataset_name: str
    input_format: str
    label_column: str | None
    source_url: str | None
    license_name: str | None
    notes: str | None
    record_count: int
    contains_input_labels: bool

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible provenance payload."""
        return {
            "dataset_name": self.dataset_name,
            "input_format": self.input_format,
            "label_column": self.label_column,
            "source_url": self.source_url,
            "license_name": self.license_name,
            "notes": self.notes,
            "record_count": self.record_count,
            "contains_input_labels": self.contains_input_labels,
        }


def _require_sklearn() -> tuple[type[Any], type[Any]]:
    """Import optional sklearn components when training is requested."""
    try:
        from sklearn.feature_extraction import DictVectorizer
        from sklearn.linear_model import LogisticRegression
    except ImportError as exc:
        raise RuntimeError(
            "ML support is not installed. Install it with "
            "`poetry install --with dev -E ml` or `pip install scikit-learn`."
        ) from exc
    return DictVectorizer, LogisticRegression


def build_parser() -> argparse.ArgumentParser:
    """Create the offline model training CLI parser."""
    parser = argparse.ArgumentParser(description="Train a baseline fraud model offline.")
    parser.add_argument("--input", type=Path, help="Optional JSONL input file.")
    parser.add_argument(
        "--users",
        type=int,
        default=20,
        help="Synthetic users when no input is given.",
    )
    parser.add_argument(
        "--transactions",
        type=int,
        default=500,
        help="Synthetic transaction count when no input is given.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Synthetic generation seed.")
    parser.add_argument(
        "--input-format",
        choices=["auto", "jsonl", "csv"],
        default="auto",
        help="Input format for --input. Defaults to suffix-based detection.",
    )
    parser.add_argument(
        "--label-column",
        default="label",
        help="Label column name for labelled input datasets.",
    )
    parser.add_argument(
        "--dataset-name",
        default="synthetic_demo",
        help="Human-readable dataset name stored in provenance metadata.",
    )
    parser.add_argument(
        "--dataset-url",
        help="Optional dataset source URL stored in provenance metadata.",
    )
    parser.add_argument(
        "--dataset-license",
        help="Optional dataset license or usage note stored in provenance metadata.",
    )
    parser.add_argument(
        "--provenance-notes",
        help="Optional free-form provenance notes stored with the model artifacts.",
    )
    parser.add_argument(
        "--require-input-labels",
        action="store_true",
        help="Fail if the input dataset does not include the requested label column.",
    )
    parser.add_argument(
        "--dataset-mapping",
        type=Path,
        help="Optional JSON mapping file for adapting external labelled datasets.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts"),
        help="Directory where model artifacts will be written.",
    )
    parser.add_argument(
        "--test-fraction",
        type=float,
        default=0.2,
        help="Fraction of examples reserved for evaluation.",
    )
    return parser


def validate_args(args: argparse.Namespace) -> None:
    """Validate CLI arguments for offline training."""
    if args.input is not None:
        if not args.input.exists():
            raise ValueError(f"input file does not exist: {args.input}")
        if not args.input.is_file():
            raise ValueError(f"input path is not a file: {args.input}")
    else:
        if args.users <= 0:
            raise ValueError("--users must be positive")
        if args.transactions <= 1:
            raise ValueError("--transactions must be greater than 1")

    if not 0 < args.test_fraction < 0.5:
        raise ValueError("--test-fraction must be between 0 and 0.5")
    if not args.label_column.strip():
        raise ValueError("--label-column must not be empty")
    if not args.dataset_name.strip():
        raise ValueError("--dataset-name must not be empty")
    if args.dataset_mapping is not None and not args.dataset_mapping.exists():
        raise ValueError(f"dataset mapping file does not exist: {args.dataset_mapping}")


def detect_input_format(input_path: Path | None, requested_format: str) -> str:
    """Resolve the effective input format for the training dataset."""
    if input_path is None:
        return "synthetic"
    if requested_format != "auto":
        return requested_format
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        return "csv"
    return "jsonl"


def build_feature_dict(features: FraudFeatures, transaction: Transaction) -> dict[str, float | str]:
    """Build an ordered raw feature dictionary for offline training."""
    values: dict[str, float | str] = {
        "amount": features.amount,
        "tx_count_5m": float(features.tx_count_5m),
        "amount_sum_1h": features.amount_sum_1h,
        "amount_zscore": features.amount_zscore,
        "minutes_since_last_tx": (
            0.0 if features.minutes_since_last_tx is None else features.minutes_since_last_tx
        ),
        "country_changed": float(features.country_changed),
        "device_changed": float(features.device_changed),
        "card_not_present": float(features.card_not_present),
        "night_transaction": float(features.night_transaction),
        "high_velocity": float(features.high_velocity),
        "high_amount": float(features.high_amount),
        "country": transaction.country,
        "merchant_category": transaction.merchant_category,
        "channel": transaction.channel,
        "is_card_present": float(transaction.is_card_present),
    }
    return {key: values[key] for key in CANONICAL_FEATURE_SCHEMA}


def validate_label_value(value: object) -> int:
    """Validate a supplied label value as a binary class."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int) and value in {0, 1}:
        return value
    if isinstance(value, str) and value in {"0", "1"}:
        return int(value)
    raise ValueError("label must be binary: 0/1 or boolean")


def derive_synthetic_label(features: FraudFeatures, transaction: Transaction) -> int:
    """Generate a transparent demo label when no real label column exists.

    This is explicitly synthetic and only meant to demonstrate the training
    pipeline. It is not real fraud ground truth.
    """
    rule_score = score_features(features)
    severe_signals = sum(
        [
            features.high_velocity,
            features.country_changed,
            features.device_changed,
            features.high_amount and features.card_not_present,
            features.amount_zscore >= DEFAULT_CONFIG.zscore_threshold,
        ]
    )
    return int(rule_score.risk_score >= 70 or severe_signals >= 2 or transaction.amount >= 900.0)


def iter_training_payloads(
    *,
    input_path: Path | None,
    input_format: str,
    dataset_mapping_path: Path | None = None,
    users: int,
    transactions: int,
    seed: int,
) -> Iterable[dict[str, Any]]:
    """Yield raw payloads from JSONL input or synthetic generation."""
    dataset_mapping = load_dataset_mapping(dataset_mapping_path)
    if input_path is not None:
        if input_format == "csv":
            with input_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames is None:
                    raise ValueError("CSV training input must include a header row")
                for _line_number, row in enumerate(reader, start=2):
                    payload = {
                        key: _coerce_csv_value(key, value)
                        for key, value in row.items()
                        if key is not None
                    }
                    if not payload:
                        continue
                    yield apply_dataset_mapping(payload, dataset_mapping)
            return
        with input_path.open("r", encoding="utf-8") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                if not isinstance(payload, dict):
                    raise ValueError(
                        f"training JSON must decode to an object at line {line_number}"
                    )
                yield apply_dataset_mapping(payload, dataset_mapping)
        return

    for payload in generate_transactions(users=users, transactions=transactions, seed=seed):
        yield payload


def _coerce_csv_value(field_name: str, value: str | None) -> Any:
    """Coerce a CSV string field into the closest canonical Python type."""
    if value is None:
        return None
    if field_name in {"amount", "latitude", "longitude"}:
        return float(value)
    if field_name == "is_card_present":
        return value.strip().lower() in {"true", "1", "yes"}
    if field_name in {"label", "fraud_flag", "is_fraud"}:
        return value.strip()
    return value


def build_training_dataset(
    payloads: Iterable[dict[str, Any]],
    *,
    label_column: str = "label",
    require_input_labels: bool = False,
    config: FraudConfig = DEFAULT_CONFIG,
) -> TrainingDataset:
    """Build an offline feature table from streaming-style feature logic."""
    states: dict[str, UserProfileState] = {}
    examples: list[TrainingExample] = []
    saw_input_label = False

    for payload in payloads:
        transaction = transaction_from_dict(payload)
        state = states.get(transaction.key, UserProfileState())
        features = compute_features(transaction, state, config)
        label_value = payload.get(label_column)
        if label_value is None:
            if require_input_labels:
                raise ValueError(
                    f"label column '{label_column}' is required but missing for "
                    f"transaction_id={transaction.transaction_id}"
                )
            label = derive_synthetic_label(features, transaction)
            label_source = "synthetic_demo_label"
        else:
            label = validate_label_value(label_value)
            label_source = "input_label"
            saw_input_label = True

        examples.append(
            TrainingExample(
                transaction_id=transaction.transaction_id,
                event_time=transaction.event_time.isoformat(),
                feature_values=build_feature_dict(features, transaction),
                label=label,
                label_source=label_source,
            )
        )
        states[transaction.key] = update_state(transaction, state, config)

    if not examples:
        raise ValueError("training dataset is empty")
    if require_input_labels and not saw_input_label:
        raise ValueError(f"label column '{label_column}' was not found in the input dataset")

    return TrainingDataset(examples=examples, feature_schema=CANONICAL_FEATURE_SCHEMA)


def _train_test_split(
    examples: list[TrainingExample],
    test_fraction: float,
) -> tuple[list[TrainingExample], list[TrainingExample]]:
    """Split examples deterministically into train and test partitions."""
    if len(examples) < 5:
        raise ValueError("at least 5 examples are required for training")

    test_size = max(1, int(len(examples) * test_fraction))
    train_size = len(examples) - test_size
    if train_size < 2:
        raise ValueError("training split is too small")
    return examples[:train_size], examples[train_size:]


def _safe_roc_auc(labels: list[int], scores: list[float]) -> float | None:
    """Compute ROC-AUC from ranks without depending on sklearn metrics."""
    positives = sum(labels)
    negatives = len(labels) - positives
    if positives == 0 or negatives == 0:
        return None

    ordered = sorted(zip(scores, labels, strict=True), key=lambda item: item[0])
    rank_sum = 0.0
    index = 0
    while index < len(ordered):
        next_index = index + 1
        while next_index < len(ordered) and ordered[next_index][0] == ordered[index][0]:
            next_index += 1
        average_rank = (index + 1 + next_index) / 2
        for _, label in ordered[index:next_index]:
            if label == 1:
                rank_sum += average_rank
        index = next_index

    return (rank_sum - positives * (positives + 1) / 2) / (positives * negatives)


def _threshold_metrics(
    labels: list[int],
    probabilities: list[float],
    threshold: float,
) -> dict[str, float]:
    """Compute precision/recall/F1 for one threshold."""
    predicted = [int(score >= threshold) for score in probabilities]
    pairs = list(zip(labels, predicted, strict=True))
    true_positive = sum(1 for label, pred in pairs if label == pred == 1)
    false_positive = sum(1 for label, pred in pairs if label == 0 and pred == 1)
    false_negative = sum(1 for label, pred in pairs if label == 1 and pred == 0)

    precision = (
        true_positive / (true_positive + false_positive)
        if (true_positive + false_positive)
        else 0.0
    )
    recall = (
        true_positive / (true_positive + false_negative)
        if (true_positive + false_negative)
        else 0.0
    )
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {
        "threshold": threshold,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def evaluate_predictions(labels: list[int], probabilities: list[float]) -> dict[str, Any]:
    """Build evaluation metrics and threshold analysis."""
    threshold_table = [
        _threshold_metrics(labels, probabilities, threshold) for threshold in (0.3, 0.5, 0.7, 0.9)
    ]
    base = _threshold_metrics(labels, probabilities, 0.5)
    roc_auc = _safe_roc_auc(labels, probabilities)
    return {
        "precision": base["precision"],
        "recall": base["recall"],
        "f1": base["f1"],
        "roc_auc": roc_auc,
        "threshold_table": threshold_table,
    }


def _vectorize_examples(
    examples: list[TrainingExample],
    vectorizer: DictTransformer,
) -> tuple[Any, list[int]]:
    """Convert raw feature dictionaries into model-ready matrices."""
    features = [example.feature_values for example in examples]
    labels = [example.label for example in examples]
    return vectorizer.fit_transform(features), labels


def _transform_examples(
    examples: list[TrainingExample],
    vectorizer: DictTransformer,
) -> tuple[Any, list[int]]:
    """Transform examples using an already fitted vectorizer."""
    features = [example.feature_values for example in examples]
    labels = [example.label for example in examples]
    return vectorizer.transform(features), labels


def save_artifacts(
    *,
    output_dir: Path,
    dataset: TrainingDataset,
    provenance: DatasetProvenance,
    model_bundle: dict[str, Any],
    metrics: dict[str, Any],
) -> TrainingArtifacts:
    """Save model, schema, and metrics under a versioned artifact directory."""
    run_dir = output_dir / datetime.now(timezone.utc).strftime("model-%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=False)

    model_path = run_dir / "model.pkl"
    feature_schema_path = run_dir / "feature_schema.json"
    metrics_path = run_dir / "metrics.json"
    provenance_path = run_dir / "dataset_provenance.json"

    with model_path.open("wb") as handle:
        pickle.dump(model_bundle, handle)

    feature_schema_path.write_text(
        json.dumps(
            {
                "raw_feature_schema": list(dataset.feature_schema),
                "vectorized_feature_names": model_bundle["vectorized_feature_names"],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    metrics_path.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
    provenance_path.write_text(
        json.dumps(provenance.to_dict(), indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return TrainingArtifacts(
        run_dir=run_dir,
        model_path=model_path,
        feature_schema_path=feature_schema_path,
        metrics_path=metrics_path,
        provenance_path=provenance_path,
    )


def train_model(args: argparse.Namespace) -> TrainingArtifacts:
    """Train the optional baseline fraud model and save artifacts."""
    DictVectorizer, LogisticRegression = _require_sklearn()
    input_format = detect_input_format(args.input, args.input_format)

    payloads = iter_training_payloads(
        input_path=args.input,
        input_format=input_format,
        dataset_mapping_path=args.dataset_mapping,
        users=args.users,
        transactions=args.transactions,
        seed=args.seed,
    )
    dataset = build_training_dataset(
        payloads,
        label_column=args.label_column,
        require_input_labels=args.require_input_labels,
    )
    train_examples, test_examples = _train_test_split(dataset.examples, args.test_fraction)

    vectorizer = DictVectorizer(sparse=False)
    classifier = LogisticRegression(max_iter=500, random_state=args.seed)

    train_X, train_y = _vectorize_examples(train_examples, vectorizer)
    classifier.fit(train_X, train_y)

    test_X, test_y = _transform_examples(test_examples, vectorizer)
    probabilities = [float(row[1]) for row in classifier.predict_proba(test_X)]
    metrics = evaluate_predictions(test_y, probabilities)
    metrics["example_count"] = len(dataset.examples)
    metrics["train_count"] = len(train_examples)
    metrics["test_count"] = len(test_examples)
    metrics["label_sources"] = sorted({example.label_source for example in dataset.examples})
    metrics["label_note"] = (
        "Synthetic demo labels were used when no input label column was present. "
        "They are only for pipeline demonstration and not real fraud ground truth."
    )
    metrics["dataset_name"] = args.dataset_name
    metrics["input_format"] = input_format

    vectorized_feature_names = list(vectorizer.vocabulary_.keys())
    model_bundle = {
        "model": classifier,
        "vectorizer": vectorizer,
        "vectorized_feature_names": vectorized_feature_names,
        "raw_feature_schema": list(dataset.feature_schema),
    }
    provenance = DatasetProvenance(
        dataset_name=args.dataset_name,
        input_format=input_format,
        label_column=(args.label_column if "input_label" in metrics["label_sources"] else None),
        source_url=args.dataset_url,
        license_name=args.dataset_license,
        notes=args.provenance_notes,
        record_count=len(dataset.examples),
        contains_input_labels="input_label" in metrics["label_sources"],
    )
    return save_artifacts(
        output_dir=args.output_dir,
        dataset=dataset,
        provenance=provenance,
        model_bundle=model_bundle,
        metrics=metrics,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the offline model training CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        validate_args(args)
        artifacts = train_model(args)
    except (RuntimeError, ValueError) as exc:
        parser.exit(status=2, message=f"{exc}\n")

    print(f"Saved model artifacts to {artifacts.run_dir}")
    print(f"Model: {artifacts.model_path}")
    print(f"Feature schema: {artifacts.feature_schema_path}")
    print(f"Metrics: {artifacts.metrics_path}")
    print(f"Dataset provenance: {artifacts.provenance_path}")
    return 0
