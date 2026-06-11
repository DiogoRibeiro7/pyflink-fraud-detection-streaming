"""Static offline analyst review UI generator."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from fraud_streaming.feedback import join_alerts_with_feedback, load_alerts, load_feedback
from fraud_streaming.schemas import Alert, AnalystFeedback


@dataclass(frozen=True, slots=True)
class ReviewUiRow:
    """One alert row rendered in the static analyst review UI."""

    transaction_id: str
    user_id: str
    event_time: str
    risk_level: str
    risk_score: int
    reasons: list[str]
    current_feedback: dict[str, str] | None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-compatible row for the embedded UI payload."""
        return {
            "transaction_id": self.transaction_id,
            "user_id": self.user_id,
            "event_time": self.event_time,
            "risk_level": self.risk_level,
            "risk_score": self.risk_score,
            "reasons": self.reasons,
            "current_feedback": self.current_feedback,
        }


def build_parser() -> argparse.ArgumentParser:
    """Create the static review UI CLI parser."""
    parser = argparse.ArgumentParser(description="Generate a static analyst review UI.")
    parser.add_argument("--alerts", type=Path, required=True, help="Alert JSONL input file.")
    parser.add_argument(
        "--feedback",
        type=Path,
        help="Optional analyst feedback JSONL file for pre-populating review state.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="HTML output path for the generated review UI.",
    )
    parser.add_argument(
        "--title",
        default="Fraud Alert Analyst Review",
        help="Page title shown in the generated UI.",
    )
    return parser


def validate_args(args: argparse.Namespace) -> None:
    """Validate review UI CLI arguments."""
    if not args.alerts.exists():
        raise ValueError(f"alerts file does not exist: {args.alerts}")
    if not args.alerts.is_file():
        raise ValueError(f"alerts path is not a file: {args.alerts}")
    if args.feedback is not None:
        if not args.feedback.exists():
            raise ValueError(f"feedback file does not exist: {args.feedback}")
        if not args.feedback.is_file():
            raise ValueError(f"feedback path is not a file: {args.feedback}")
    if args.output.parent != Path():
        args.output.parent.mkdir(parents=True, exist_ok=True)


def build_review_rows(
    alerts: Sequence[Alert], feedback_rows: Sequence[AnalystFeedback]
) -> list[ReviewUiRow]:
    """Combine alerts with latest feedback for UI rendering."""
    joined_rows, _unmatched = join_alerts_with_feedback(alerts, feedback_rows)
    rows: list[ReviewUiRow] = []
    for joined in joined_rows:
        current_feedback = None
        if joined.feedback is not None:
            current_feedback = {
                "reviewer_id": joined.feedback.reviewer_id,
                "label": joined.feedback.label,
                "comment": joined.feedback.comment,
                "reviewed_at": joined.feedback.reviewed_at.isoformat(),
            }
        rows.append(
            ReviewUiRow(
                transaction_id=joined.alert.transaction_id,
                user_id=joined.alert.user_id,
                event_time=joined.alert.event_time.isoformat(),
                risk_level=joined.alert.risk_level,
                risk_score=joined.alert.risk_score,
                reasons=joined.alert.reasons,
                current_feedback=current_feedback,
            )
        )
    return rows


def render_review_ui_html(rows: Sequence[ReviewUiRow], title: str) -> str:
    """Render a self-contained HTML analyst review UI."""
    payload = json.dumps([row.to_dict() for row in rows], sort_keys=True)
    safe_title = title.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{safe_title}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f5f1e8;
      --panel: #fffdf8;
      --ink: #202020;
      --muted: #6b655a;
      --line: #d8d0c2;
      --accent: #0f766e;
      --high: #b91c1c;
      --medium: #c2410c;
      --elevated: #9a6700;
      --low: #3f6212;
    }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background:
        radial-gradient(circle at top right, rgba(15, 118, 110, 0.08), transparent 24rem),
        linear-gradient(180deg, #f3ecdf 0%, var(--bg) 100%);
      color: var(--ink);
    }}
    main {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 2rem 1rem 4rem;
    }}
    .hero {{
      background: var(--panel);
      border: 1px solid var(--line);
      box-shadow: 0 14px 30px rgba(32, 32, 32, 0.08);
      padding: 1.5rem;
      margin-bottom: 1rem;
    }}
    .hero h1 {{
      margin: 0 0 0.5rem;
      font-size: clamp(2rem, 4vw, 3rem);
    }}
    .hero p {{
      margin: 0;
      color: var(--muted);
      max-width: 72ch;
      line-height: 1.45;
    }}
    .controls {{
      display: grid;
      gap: 0.75rem;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      margin: 1rem 0;
    }}
    .controls label {{
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
      font-size: 0.95rem;
      color: var(--muted);
    }}
    input, select, textarea, button {{
      font: inherit;
      padding: 0.65rem 0.75rem;
      border: 1px solid var(--line);
      background: var(--panel);
      color: var(--ink);
    }}
    button {{
      cursor: pointer;
      background: var(--accent);
      color: white;
      border-color: var(--accent);
    }}
    .summary {{
      display: flex;
      gap: 1rem;
      flex-wrap: wrap;
      margin: 1rem 0;
    }}
    .chip {{
      background: var(--panel);
      border: 1px solid var(--line);
      padding: 0.65rem 0.8rem;
      min-width: 8rem;
    }}
    .layout {{
      display: grid;
      gap: 1rem;
      grid-template-columns: minmax(0, 2fr) minmax(320px, 1fr);
      align-items: start;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: var(--panel);
      border: 1px solid var(--line);
    }}
    th, td {{
      padding: 0.75rem;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
      text-align: left;
    }}
    th {{
      font-size: 0.9rem;
      color: var(--muted);
    }}
    tr[selected-row="true"] {{
      outline: 2px solid var(--accent);
      outline-offset: -2px;
      background: rgba(15, 118, 110, 0.05);
    }}
    .risk {{
      display: inline-block;
      padding: 0.15rem 0.45rem;
      border: 1px solid currentColor;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-size: 0.78rem;
    }}
    .risk-high {{ color: var(--high); }}
    .risk-medium {{ color: var(--medium); }}
    .risk-elevated {{ color: var(--elevated); }}
    .risk-low {{ color: var(--low); }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      padding: 1rem;
    }}
    .panel h2 {{
      margin-top: 0;
    }}
    .reasons {{
      margin: 0;
      padding-left: 1rem;
      color: var(--muted);
    }}
    .footer-note {{
      margin-top: 1rem;
      color: var(--muted);
      line-height: 1.45;
      font-size: 0.92rem;
    }}
    @media (max-width: 960px) {{
      .layout {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <h1>{safe_title}</h1>
      <p>
        Review alerts offline, prefill decisions from existing analyst feedback, and export
        canonical feedback JSONL records compatible with <code>fraud-feedback-report</code>.
      </p>
    </section>

    <section class="controls">
      <label>Filter by risk level
        <select id="risk-filter">
          <option value="all">All</option>
          <option value="high">High</option>
          <option value="medium">Medium</option>
          <option value="elevated">Elevated</option>
          <option value="low">Low</option>
        </select>
      </label>
      <label>Search transaction or user
        <input id="search-filter" type="search" placeholder="tx-000123 or user-001">
      </label>
      <label>Show
        <select id="review-filter">
          <option value="all">All alerts</option>
          <option value="reviewed">Reviewed only</option>
          <option value="unreviewed">Unreviewed only</option>
        </select>
      </label>
      <label>Reviewer ID
        <input id="reviewer-id" type="text" placeholder="analyst-1">
      </label>
    </section>

    <section class="summary" id="summary"></section>

    <section class="layout">
      <div>
        <table>
          <thead>
            <tr>
              <th>Transaction</th>
              <th>Risk</th>
              <th>Reasons</th>
              <th>Feedback</th>
            </tr>
          </thead>
          <tbody id="alert-rows"></tbody>
        </table>
      </div>
      <aside class="panel">
        <h2>Review Form</h2>
        <p id="selected-transaction">Select an alert row to review.</p>
        <label>Label
          <select id="label-input">
            <option value="needs_review">needs_review</option>
            <option value="true_fraud">true_fraud</option>
            <option value="false_positive">false_positive</option>
          </select>
        </label>
        <label>Comment
          <textarea
            id="comment-input"
            rows="6"
            placeholder="Why is this alert correct or incorrect?"
          ></textarea>
        </label>
        <button id="save-review" type="button">Save Review In Browser</button>
        <button id="download-feedback" type="button">Download Feedback JSONL</button>
        <p class="footer-note">
          Reviews are stored only in this browser session until you export them.
          The downloaded JSONL can be fed back into <code>fraud-feedback-report</code>.
        </p>
      </aside>
    </section>
  </main>
  <script>
    const rows = {payload};
    const state = {{
      selectedId: rows.length ? rows[0].transaction_id : null,
      reviews: new Map(
        rows
          .filter(row => row.current_feedback !== null)
          .map(row => [row.transaction_id, row.current_feedback])
      ),
    }};

    const riskFilter = document.getElementById("risk-filter");
    const searchFilter = document.getElementById("search-filter");
    const reviewFilter = document.getElementById("review-filter");
    const reviewerInput = document.getElementById("reviewer-id");
    const labelInput = document.getElementById("label-input");
    const commentInput = document.getElementById("comment-input");
    const selectedTransaction = document.getElementById("selected-transaction");
    const alertRows = document.getElementById("alert-rows");
    const summary = document.getElementById("summary");

    function feedbackFor(transactionId) {{
      return state.reviews.get(transactionId) || null;
    }}

    function filteredRows() {{
      const riskValue = riskFilter.value;
      const searchValue = searchFilter.value.trim().toLowerCase();
      const reviewValue = reviewFilter.value;
      return rows.filter(row => {{
        if (riskValue !== "all" && row.risk_level !== riskValue) {{
          return false;
        }}
        if (
          searchValue &&
          !row.transaction_id.toLowerCase().includes(searchValue) &&
          !row.user_id.toLowerCase().includes(searchValue)
        ) {{
          return false;
        }}
        const hasFeedback = feedbackFor(row.transaction_id) !== null;
        if (reviewValue === "reviewed" && !hasFeedback) {{
          return false;
        }}
        if (reviewValue === "unreviewed" && hasFeedback) {{
          return false;
        }}
        return true;
      }});
    }}

    function renderSummary() {{
      const current = filteredRows();
      const reviewed = current.filter(row => feedbackFor(row.transaction_id) !== null).length;
      const byRisk = current.reduce((acc, row) => {{
        acc[row.risk_level] = (acc[row.risk_level] || 0) + 1;
        return acc;
      }}, {{}});
      const chips = [
        ["Visible alerts", current.length],
        ["Reviewed", reviewed],
        ["Unreviewed", current.length - reviewed],
        ...Object.entries(byRisk).map(([key, value]) => [key, value]),
      ];
      summary.innerHTML = chips
        .map(
          ([label, value]) =>
            `<div class="chip"><strong>${{value}}</strong><br>${{label}}</div>`
        )
        .join("");
    }}

    function renderRows() {{
      const current = filteredRows();
      alertRows.innerHTML = current.map(row => {{
        const feedback = feedbackFor(row.transaction_id);
        const selected = row.transaction_id === state.selectedId;
        const feedbackText = feedback
          ? `${{feedback.label}} by ${{feedback.reviewer_id}}`
          : "not reviewed";
        return `
          <tr data-transaction-id="${{row.transaction_id}}" selected-row="${{selected}}">
            <td>
              <strong>${{row.transaction_id}}</strong><br>
              <span>${{row.user_id}}</span><br>
              <span>${{row.event_time}}</span>
            </td>
            <td>
              <span class="risk risk-${{row.risk_level}}">${{row.risk_level}}</span><br>
              score=${{row.risk_score}}
            </td>
            <td>
              <ul class="reasons">
                ${{
                  row.reasons.map(reason => `<li>${{reason}}</li>`).join("")
                }}
              </ul>
            </td>
            <td>${{feedbackText}}</td>
          </tr>
        `;
      }}).join("");

      for (const tr of alertRows.querySelectorAll("tr")) {{
        tr.addEventListener("click", () => {{
          state.selectedId = tr.dataset.transactionId;
          syncSelectedReview();
          renderRows();
        }});
      }}
    }}

    function syncSelectedReview() {{
      const row = rows.find(item => item.transaction_id === state.selectedId);
      if (!row) {{
        selectedTransaction.textContent = "Select an alert row to review.";
        return;
      }}
      selectedTransaction.textContent =
        `${{row.transaction_id}} for ${{row.user_id}} at risk=${{row.risk_score}}`;
      const feedback = feedbackFor(row.transaction_id);
      labelInput.value = feedback ? feedback.label : "needs_review";
      commentInput.value = feedback ? feedback.comment : "";
      if (feedback && !reviewerInput.value) {{
        reviewerInput.value = feedback.reviewer_id;
      }}
    }}

    function saveCurrentReview() {{
      if (!state.selectedId) {{
        return;
      }}
      const reviewerId = reviewerInput.value.trim();
      if (!reviewerId) {{
        window.alert("Reviewer ID is required before saving feedback.");
        return;
      }}
      state.reviews.set(state.selectedId, {{
        reviewer_id: reviewerId,
        label: labelInput.value,
        comment: commentInput.value.trim(),
        reviewed_at: new Date().toISOString(),
      }});
      renderSummary();
      renderRows();
    }}

    function downloadFeedback() {{
      const lines = Array.from(state.reviews.entries())
        .sort((left, right) => left[0].localeCompare(right[0]))
        .map(([transactionId, feedback]) => JSON.stringify({{
          transaction_id: transactionId,
          reviewer_id: feedback.reviewer_id,
          label: feedback.label,
          comment: feedback.comment,
          reviewed_at: feedback.reviewed_at,
        }}));
      const blob = new Blob([lines.join("\\n")], {{ type: "application/x-ndjson" }});
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = "analyst_feedback.jsonl";
      link.click();
      URL.revokeObjectURL(url);
    }}

    riskFilter.addEventListener("change", () => {{ renderSummary(); renderRows(); }});
    reviewFilter.addEventListener("change", () => {{ renderSummary(); renderRows(); }});
    searchFilter.addEventListener("input", () => {{ renderSummary(); renderRows(); }});
    document.getElementById("save-review").addEventListener("click", saveCurrentReview);
    document.getElementById("download-feedback").addEventListener("click", downloadFeedback);

    renderSummary();
    renderRows();
    syncSelectedReview();
  </script>
</body>
</html>
"""


def save_review_ui(path: Path, html: str) -> None:
    """Persist the rendered review UI to disk."""
    path.write_text(html, encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the static review UI generator CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        validate_args(args)
        alerts = load_alerts(args.alerts)
        feedback_rows = [] if args.feedback is None else load_feedback(args.feedback)
        rows = build_review_rows(alerts, feedback_rows)
        save_review_ui(args.output, render_review_ui_html(rows, args.title))
    except ValueError as exc:
        parser.exit(status=2, message=f"{exc}\n")

    print(f"Saved analyst review UI to {args.output}")
    return 0
