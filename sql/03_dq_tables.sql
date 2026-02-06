/* ============================================================
   03_dq_tables.sql  (DuckDB)
   ------------------------------------------------------------
   PURPOSE:
   Create the "monitoring tables" where our monitoring system
   stores results every day.

   Think of these tables as "logs":
   - dq_check_results = daily report card (PASS/FAIL/WARN)
   - dq_anomalies     = KPI spikes/drops vs baseline
   - dq_incidents     = human workflow tracking (open/close, root cause)
   ============================================================ */


-- ------------------------------------------------------------
-- 0) Create a separate schema "dq" to keep monitoring tables
--    away from core analytics tables.
--    (If you prefer, you can skip schema and create tables in main.)
-- ------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS dq;


-- ------------------------------------------------------------
-- 1) dq.dq_check_results
-- ------------------------------------------------------------
-- This is the most important table initially.
-- Every day, each check writes 1 row here.
--
-- Example rows:
-- check_date = 2026-02-04
-- check_name = 'freshness_latest_event_within_24h'
-- status     = 'FAIL'
-- failure_details = 'Latest event_ts older than 24h'
--
CREATE TABLE IF NOT EXISTS dq.dq_check_results (
  check_date       DATE,        -- what day the check is for
  check_name       VARCHAR,     -- unique check identifier (name)
  table_name       VARCHAR,     -- which table the check ran on
  severity         VARCHAR,     -- P0/P1/P2 (P0 = most critical)
  status           VARCHAR,     -- PASS/FAIL/WARN
  metric_value     DOUBLE,      -- numeric output of the check (count/percent/timestamp)
  expected_range   VARCHAR,     -- human readable threshold (e.g. '>=1000')
  failure_details  VARCHAR,     -- short explanation of what failed
  created_at       TIMESTAMP DEFAULT now() -- when the row was inserted
);


-- ------------------------------------------------------------
-- 2) dq.dq_anomalies
-- ------------------------------------------------------------
-- This stores anomalies in KPI metrics.
-- It answers: "Did revenue/purchases/events behave strangely?"
--
-- Baseline logic (we implement in 05_anomalies.sql):
-- - baseline = rolling 14-day mean/median
-- - anomaly if z_score > 3 OR pct_change exceeds threshold
--
CREATE TABLE IF NOT EXISTS dq.dq_anomalies (
  anomaly_date     DATE,        -- date of the anomaly (usually yesterday)
  entity_type      VARCHAR,     -- overall/category/product/etc
  entity_id        VARCHAR,     -- nullable: category_id/product_id/etc
  metric_name      VARCHAR,     -- revenue / purchases / events_count etc
  actual_value     DOUBLE,      -- actual observed value
  baseline_value   DOUBLE,      -- baseline (rolling mean/median)
  z_score          DOUBLE,      -- standardized deviation (optional sometimes NULL)
  pct_change       DOUBLE,      -- change vs previous day (optional sometimes NULL)
  anomaly_flag     VARCHAR,     -- Y/N
  notes            VARCHAR,     -- short notes / reason
  created_at       TIMESTAMP DEFAULT now()
);


-- ------------------------------------------------------------
-- 3) dq.dq_incidents
-- ------------------------------------------------------------
-- This is manual or semi-automatic.
-- When something FAILS, you log an incident:
-- - what happened
-- - severity
-- - status (OPEN -> RESOLVED)
-- - root cause + resolution steps
--
CREATE TABLE IF NOT EXISTS dq.dq_incidents (
  incident_id        VARCHAR,     -- string ID like 'INC-0001' or UUID
  opened_at          TIMESTAMP,
  closed_at          TIMESTAMP,
  severity           VARCHAR,     -- P0/P1/P2
  status             VARCHAR,     -- OPEN/TRIAGING/MITIGATED/RESOLVED
  summary            VARCHAR,     -- short description of issue
  detected_by        VARCHAR,     -- which check/anomaly detected it
  impacted_assets    VARCHAR,     -- dashboards/tables impacted (text)
  root_cause_type    VARCHAR,     -- enum-like: upstream_missing/schema_drift/logic_bug...
  root_cause_details VARCHAR,     -- explanation
  resolution_steps   VARCHAR,     -- how it was fixed
  preventive_actions VARCHAR,     -- what we’ll do to avoid this again
  owner              VARCHAR      -- who owns the incident
);

CREATE TABLE IF NOT EXISTS dq.dq_run_context (
  run_date     DATE,
  max_event_ts TIMESTAMP,
  created_at   TIMESTAMP DEFAULT now()
);
