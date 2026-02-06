
---

## 4) `docs/overview.md` ✅ (create inside `/docs`)

```md
# Overview: Ops DQ + Incident Monitoring

## Goal
Simulate how an analytics / data platform team runs:
- Daily reliability checks (freshness, volume, null %, duplicates)
- Incident creation when checks fail (P0/P1 severity)
- Monitoring root causes and recurring failure patterns
- Detecting KPI anomalies via rolling baselines

This repo is intentionally “ops-grade”: it tracks failures, incidents, and prevention actions.

---

## Data Flow
1) **Ingest (Replay)**
   - Load all history into `core.fact_events_all`
   - Replay inserts one day into `core.fact_events` (simulating daily pipeline)

2) **DQ Checks**
   - Run checks for `run_date` using `TEMP VIEW params(run_date, prev_date)`
   - Write results to `dq.dq_check_results` (idempotent per day)

3) **Incidents**
   - Incidents are recorded in `dq.dq_incidents`
   - Visualized as an ops queue (OPEN incidents)

4) **Anomalies**
   - Compute daily KPIs from `core.fact_events`
   - Use 14-day rolling baseline (mean + stddev)
   - Flag anomalies with z-score / % change thresholds
   - Write into `dq.dq_anomalies`

5) **Exports for Power BI**
   - Export curated views/tables as CSV into `data/output/`

---

## DQ Severity Guide
- **P0** = production-breaking / urgent (freshness, volume collapse, duplicates)
- **P1** = high priority (null spikes, schema/logging degradation)

---

## What to demo in an interview
- A specific failure (e.g., session_id null spike ~20%)
- How the check flags it (P1 FAIL + metric_value)
- Incident created (OPEN) with root cause + preventive actions
- Root cause analysis dashboard

---

## Where things live
- SQL scripts: `sql/`
- Runner: `scripts/run_daily.py`
- Power BI report: `powerbi/`
- Output CSVs: `data/output/`
- Screenshots: `assets/screenshots/`
