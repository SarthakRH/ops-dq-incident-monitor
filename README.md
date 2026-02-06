# Ops DQ + Incident Monitoring (DuckDB + SQL + Power BI)

A production-style **data reliability + incident operations** project:
- **Replayable daily ingestion** into a “daily fact” table (simulates a real pipeline run)
- **Data Quality (DQ) checks** (P0/P1) written into governed DQ tables
- **Incident workflow** (OPEN/RESOLVED) with root cause + preventive actions
- **Anomaly detection** using rolling baselines (z-score + % change)
- **Power BI ops dashboards**: health, check drilldown, incident queue, root cause analysis

---

## Why this project is “ops-grade”
Most dashboards only show charts. This repo shows how analytics teams actually operate:
- Detect failures (DQ checks)
- Track incidents and ownership
- Diagnose root causes + recurring patterns
- Capture preventative actions
- Maintain replayable runs for debugging and demos

---

## Power BI Pages (what to look at)
Screenshots: `assets/screenshots/`

1) **Data Health Monitoring**
   - Daily health score
   - P0/P1 issue counts
   - Fail/Warn trend

2) **Check Detail (drilldown)**
   - Filter by `check_name` + `check_date`
   - View `metric_value`, expected range, and failure details
   - Trend chart for the selected check

3) **Incident Tracker (ops queue)**
   - OPEN incidents (default filter)
   - Sort by severity (P0 first), then latest opened
   - Related failing checks table (shows what triggered the incident)

4) **Root Causes**
   - Incident count by `root_cause_type`
   - Cumulative % (Pareto-style)
   - Top failing checks (FailWarn Days / Rows)

---

## Repo Structure
```text
sql/        -> SQL scripts (load, replay, dq tables, checks, anomalies, exports)
scripts/    -> Python runner(s)
powerbi/    -> PBIX + theme + background image
data/
  raw/      -> (ignored) optional place for large raw files
  staged/   -> (ignored) optional staging area
  output/   -> exported CSVs used by Power BI (tracked; small)
assets/
  screenshots/ -> dashboard screenshots (tracked)
docs/       -> deeper documentation (architecture/runbook)
db/         -> local DuckDB file (ignored)
