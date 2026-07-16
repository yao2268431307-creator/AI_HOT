from __future__ import annotations

from statistics import median


def review_funnel(rows: list[dict[str, object]]) -> dict[str, object]:
    opened: dict[tuple[str, str], object] = {}
    durations: list[float] = []
    triage = 0
    accepted = 0
    for row in sorted(rows, key=lambda item: item["occurredAt"]):
        key = (str(row["sessionId"]), str(row.get("eventId") or ""))
        if row["kind"] == "detail_opened":
            opened.setdefault(key, row["occurredAt"])
        elif row["kind"] == "triage_submitted":
            triage += 1
            metadata = row.get("metadata") or {}
            if isinstance(metadata, dict) and metadata.get("action") == "confirm":
                accepted += 1
            started = opened.get(key)
            if started is not None:
                durations.append(max(0, (row["occurredAt"] - started).total_seconds()))
    return {
        "metricScope": "exploratory_review_funnel_not_rc2_beta_kpi",
        "detailOpenSample": len({(str(row["sessionId"]), str(row.get("eventId") or "")) for row in rows if row["kind"] == "detail_opened"}),
        "triageSample": triage,
        "timedTriageFromDetailOpenSample": len(durations),
        "triageConfirmRate": accepted / triage if triage else None,
        "triageWithin15MinutesFromDetailOpenRate": sum(value <= 900 for value in durations) / len(durations) if durations else None,
        "medianReviewSecondsFromDetailOpen": median(durations) if durations else None,
        "limitations": [
            "This is not strong-alert acceptance: its denominator is submitted triage decisions, not delivered strong alerts.",
            "The 15-minute clock starts at detail open, not queue eligibility, and does not apply duty-window exclusions.",
        ],
    }
