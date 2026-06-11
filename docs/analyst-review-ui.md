# Analyst Review UI

The repository now includes a lightweight offline review UI generator:

```bash
poetry run fraud-review-ui \
  --alerts data/fraud_alerts.jsonl \
  --feedback data/analyst_feedback.jsonl \
  --output artifacts/review.html
```

The generated HTML file is self-contained:

- no web server is required
- no JavaScript build step is required
- no new frontend dependency is introduced

## What it does

- loads canonical alert JSONL output
- optionally preloads the latest analyst feedback per transaction
- lets a reviewer filter alerts by risk level or review status
- lets a reviewer add or edit labels and comments in the browser
- exports canonical feedback JSONL that can be fed into `fraud-feedback-report`

## What it does not do

- it is not a multi-user review application
- it does not persist edits to a database
- it does not attempt authentication or access control

This is deliberate. The goal is to provide a portfolio-grade analyst review surface without turning the repository into a full web product.
