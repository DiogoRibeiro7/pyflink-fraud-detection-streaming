# Public Labelled Dataset Evaluation

This repository does not bundle any public fraud dataset. That is intentional:

- many public fraud datasets have license or redistribution limits
- some widely copied examples come from Kaggle and require manual acceptance of usage terms
- the checked-in synthetic data in this repo is only for demonstration

What the code now supports is a documented path for evaluating the baseline model on a labelled dataset that you supply locally.

## Supported input shapes

`fraud-train-model` now accepts:

- JSONL transaction input, the existing path
- CSV transaction input with a configurable label column

For labelled datasets, pass:

- `--input PATH`
- `--input-format csv` when the suffix is not enough
- `--label-column COLUMN_NAME`
- `--dataset-name NAME`
- optional provenance flags such as `--dataset-url`, `--dataset-license`, and `--provenance-notes`
- `--require-input-labels` if the run must fail rather than fall back to synthetic demo labels

Each training run now saves `dataset_provenance.json` next to the model, schema, and metrics artifacts.

## Example: Kaggle Credit Card Fraud Detection

One common public benchmark is the Kaggle credit-card fraud dataset:

- https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud

This repository does not download it automatically and does not commit it.

Important caveat:

- that dataset contains pre-engineered PCA features rather than the transaction fields used by this streaming demo
- it is useful for a labelled-model benchmark, but it is not a direct substitute for the canonical transaction schema in this repo

For a schema-aligned evaluation, use a labelled dataset that includes fields such as:

- `transaction_id`
- `user_id`
- `card_id`
- `merchant_id`
- `amount`
- `currency`
- `country`
- `device_id`
- `merchant_category`
- `event_time`
- `channel`
- `is_card_present`
- a binary label column

## Example command

```bash
poetry install --with dev -E ml
poetry run fraud-train-model \
  --input data/public/labelled_transactions.csv \
  --input-format csv \
  --label-column is_fraud \
  --dataset-name demo-public-labelled-set \
  --dataset-url https://example.com/dataset-card-fraud \
  --dataset-license "See upstream dataset terms" \
  --provenance-notes "Manual local download; not committed to git." \
  --require-input-labels \
  --output-dir artifacts
```

## Interpretation notes

- Synthetic demo labels are still available when no label column is present, but they are explicitly marked as synthetic in the metrics output.
- Public labelled evaluation should be treated as a benchmark of the current feature pipeline, not as proof of production fraud performance.
- Keep provenance notes precise enough that another reviewer can understand where the labels came from and what restrictions apply.
