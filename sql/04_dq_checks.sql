/* ============================================================
   04_dq_checks.sql (DuckDB)
   PURPOSE:
   Run reliability checks for the run_date in TEMP VIEW params
   and write results into dq.dq_check_results (replay-friendly).
   REQUIREMENT:
   TEMP VIEW params(run_date, prev_date) MUST already exist.
   ============================================================ */

-- Guard (will error if params missing)
SELECT run_date, prev_date FROM params;

-- Idempotency: delete existing results for that run_date
DELETE FROM dq.dq_check_results
WHERE check_date = (SELECT run_date FROM params);

-- ------------------------------------------------------------
-- CHECK 1: Freshness = do we have events on run_date? (P0)
-- ------------------------------------------------------------
INSERT INTO dq.dq_check_results
(check_date, check_name, table_name, severity, status, metric_value, expected_range, failure_details, created_at)
SELECT
  (SELECT run_date FROM params) AS check_date,
  'freshness_has_events_on_run_date' AS check_name,
  'core.fact_events' AS table_name,
  'P0' AS severity,
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*)::DOUBLE AS metric_value,
  '> 0 rows on run_date' AS expected_range,
  CASE WHEN COUNT(*) > 0 THEN NULL ELSE 'No events found for run_date' END AS failure_details,
  now() AS created_at
FROM core.fact_events
WHERE date(event_ts) = (SELECT run_date FROM params);

-- ------------------------------------------------------------
-- CHECK 2: Volume previous day >= 1000 (P0)
-- ------------------------------------------------------------
WITH y AS (
  SELECT COUNT(*)::DOUBLE AS cnt
  FROM core.fact_events
  WHERE date(event_ts) = (SELECT prev_date FROM params)
)
INSERT INTO dq.dq_check_results
(check_date, check_name, table_name, severity, status, metric_value, expected_range, failure_details, created_at)
SELECT
  (SELECT run_date FROM params),
  'volume_prev_day_events_min_1000',
  'core.fact_events',
  'P0',
  CASE WHEN cnt >= 1000 THEN 'PASS' ELSE 'FAIL' END,
  cnt,
  '>= 1000',
  CASE WHEN cnt >= 1000 THEN NULL ELSE 'Previous day events below 1000' END,
  now()
FROM y;

-- ------------------------------------------------------------
-- CHECK 3: Purchase vanish previous day (P0)
-- ------------------------------------------------------------
WITH p AS (
  SELECT COUNT(*)::DOUBLE AS purchases
  FROM core.fact_events
  WHERE event_type='purchase'
    AND date(event_ts) = (SELECT prev_date FROM params)
)
INSERT INTO dq.dq_check_results
(check_date, check_name, table_name, severity, status, metric_value, expected_range, failure_details, created_at)
SELECT
  (SELECT run_date FROM params),
  'purchase_events_vanish_prev_day',
  'core.fact_events',
  'P0',
  CASE WHEN purchases > 0 THEN 'PASS' ELSE 'FAIL' END,
  purchases,
  '> 0',
  CASE WHEN purchases > 0 THEN NULL ELSE 'No purchases on previous day' END,
  now()
FROM p;

-- ------------------------------------------------------------
-- CHECK 4: Null session_id % on run_date <= 1% (P1)
-- ------------------------------------------------------------
WITH s AS (
  SELECT
    100.0 * SUM(CASE WHEN session_id IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0) AS null_pct
  FROM core.fact_events
  WHERE date(event_ts) = (SELECT run_date FROM params)
)
INSERT INTO dq.dq_check_results
(check_date, check_name, table_name, severity, status, metric_value, expected_range, failure_details, created_at)
SELECT
  (SELECT run_date FROM params),
  'null_rate_session_id_run_date',
  'core.fact_events',
  'P1',
  CASE WHEN null_pct <= 1.0 THEN 'PASS' ELSE 'FAIL' END,
  null_pct,
  '<= 1%',
  CASE WHEN null_pct <= 1.0 THEN NULL ELSE 'session_id null% exceeded 1% on run_date' END,
  now()
FROM s;
