-- /* ============================================================
--    05_anomalies.sql (DuckDB)
--    ------------------------------------------------------------
--    PURPOSE:
--    1) Build daily KPI series from core.fact_events
--    2) Compute rolling 14-day baseline (mean + stddev)
--    3) Flag anomalies using z-score and % change thresholds
--    4) Insert flagged metrics into dq.dq_anomalies (for yesterday)
--    ============================================================ */

-- -- -------------------------
-- WITH params AS (
--   SELECT DATE '2019-11-15' AS run_date   -- <-- CHANGE THIS when you want
-- )
-- -- Optional: prevent duplicate anomaly rows if re-running today
-- DELETE FROM dq.dq_anomalies
-- WHERE anomaly_date = (SELECT run_date FROM params);



-- WITH daily_kpis AS (
--   SELECT
--     date(event_ts) AS d,
--     count(*)::DOUBLE AS events_count,
--     sum(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END)::DOUBLE AS purchases,
--     sum(CASE WHEN event_type='purchase' THEN price ELSE 0 END)::DOUBLE AS revenue
--   FROM core.fact_events
--   GROUP BY 1
-- ),

-- kpis_enriched AS (
--   SELECT
--     d,
--     events_count,
--     purchases,
--     revenue,

--     -- Conversion proxy: purchases / events (simple; later you can use funnel table)
--     CASE WHEN events_count=0 THEN NULL ELSE purchases / events_count END AS cvr,

--     -- Rolling 14-day baselines (exclude current day: use 14 preceding to 1 preceding)
--     avg(events_count) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS base_events,
--     stddev_samp(events_count) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS sd_events,

--     avg(revenue) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS base_revenue,
--     stddev_samp(revenue) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS sd_revenue,

--     avg(cvr) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS base_cvr,
--     stddev_samp(cvr) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS sd_cvr,

--     -- Previous day values for pct change
--     lag(events_count) OVER (ORDER BY d) AS prev_events,
--     lag(revenue)      OVER (ORDER BY d) AS prev_revenue,
--     lag(cvr)          OVER (ORDER BY d) AS prev_cvr
--   FROM daily_kpis
-- ),

-- flags AS (
--   -- EVENTS COUNT
--   SELECT
--     d AS anomaly_date,
--     'overall' AS entity_type,
--     NULL AS entity_id,
--     'events_count' AS metric_name,
--     events_count AS actual_value,
--     base_events AS baseline_value,

--     CASE WHEN sd_events IS NULL OR sd_events=0 THEN NULL
--          ELSE (events_count - base_events) / sd_events END AS z_score,

--     CASE WHEN prev_events IS NULL OR prev_events=0 THEN NULL
--          ELSE (events_count - prev_events) / prev_events END AS pct_change,

--     CASE
--       WHEN base_events IS NULL THEN 'N'
--       WHEN abs((events_count - base_events) / NULLIF(sd_events,0)) > 3 THEN 'Y'
--       WHEN abs((events_count - prev_events) / NULLIF(prev_events,0)) > 0.60 THEN 'Y'
--       ELSE 'N'
--     END AS anomaly_flag,

--     'z>|3| OR day_change>|60%|' AS notes
--   FROM kpis_enriched

--   UNION ALL

--   -- REVENUE
--   SELECT
--     d,
--     'overall',
--     NULL,
--     'revenue',
--     revenue,
--     base_revenue,
--     CASE WHEN sd_revenue IS NULL OR sd_revenue=0 THEN NULL
--          ELSE (revenue - base_revenue) / sd_revenue END,
--     CASE WHEN prev_revenue IS NULL OR prev_revenue=0 THEN NULL
--          ELSE (revenue - prev_revenue) / prev_revenue END,
--     CASE
--       WHEN base_revenue IS NULL THEN 'N'
--       WHEN abs((revenue - base_revenue) / NULLIF(sd_revenue,0)) > 3 THEN 'Y'
--       WHEN (revenue - prev_revenue) / NULLIF(prev_revenue,0) > 0.60 THEN 'Y'
--       WHEN (revenue - prev_revenue) / NULLIF(prev_revenue,0) < -0.40 THEN 'Y'
--       ELSE 'N'
--     END,
--     'z>|3| OR +60% / -40% day_change'
--   FROM kpis_enriched

--   UNION ALL

--   -- CVR (purchase/event proxy)
--   SELECT
--     d,
--     'overall',
--     NULL,
--     'cvr',
--     cvr,
--     base_cvr,
--     CASE WHEN sd_cvr IS NULL OR sd_cvr=0 THEN NULL
--          ELSE (cvr - base_cvr) / sd_cvr END,
--     CASE WHEN prev_cvr IS NULL OR prev_cvr=0 THEN NULL
--          ELSE (cvr - prev_cvr) / prev_cvr END,
--     CASE
--       WHEN base_cvr IS NULL THEN 'N'
--       WHEN abs((cvr - base_cvr) / NULLIF(sd_cvr,0)) > 3 THEN 'Y'
--       WHEN abs((cvr - prev_cvr) / NULLIF(prev_cvr,0)) > 0.50 THEN 'Y'
--       ELSE 'N'
--     END,
--     'z>|3| OR day_change>|50%|'
--   FROM kpis_enriched
-- )

-- INSERT INTO dq.dq_anomalies
-- SELECT
--   anomaly_date,
--   entity_type,
--   entity_id,
--   metric_name,
--   actual_value,
--   baseline_value,
--   z_score,
--   pct_change,
--   anomaly_flag,
--   notes,
--   now() AS created_at
-- FROM flags
-- WHERE anomaly_date = (SELECT MAX(date(event_ts)) FROM core.fact_events);
-- -- WHERE anomaly_date = (SELECT run_date FROM params);

/* ============================================================
   05_anomalies.sql (DuckDB)
   PURPOSE:
   Build daily KPIs, compute rolling 14-day baseline, flag anomalies,
   and insert results for a chosen run_date (replay-friendly).
   ============================================================ */

-- =========================
-- 1) DELETE for run_date
-- =========================
WITH params AS (
  SELECT DATE '2019-11-15' AS run_date      -- <-- CHANGE THIS DATE
)
DELETE FROM dq.dq_anomalies
WHERE anomaly_date = (SELECT run_date FROM params);

-- =========================
-- 2) INSERT for run_date
-- =========================
WITH params AS (
  SELECT DATE '2019-11-15' AS run_date      -- <-- SAME DATE HERE TOO
),
daily_kpis AS (
  SELECT
    date(event_ts) AS d,
    count(*)::DOUBLE AS events_count,
    sum(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END)::DOUBLE AS purchases,
    sum(CASE WHEN event_type='purchase' THEN price ELSE 0 END)::DOUBLE AS revenue
  FROM core.fact_events
  GROUP BY 1
),
kpis_enriched AS (
  SELECT
    d,
    events_count,
    purchases,
    revenue,
    CASE WHEN events_count=0 THEN NULL ELSE purchases / events_count END AS cvr,

    avg(events_count) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS base_events,
    stddev_samp(events_count) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS sd_events,

    avg(revenue) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS base_revenue,
    stddev_samp(revenue) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS sd_revenue,

    avg(cvr) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS base_cvr,
    stddev_samp(cvr) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS sd_cvr,

    lag(events_count) OVER (ORDER BY d) AS prev_events,
    lag(revenue)      OVER (ORDER BY d) AS prev_revenue,
    lag(cvr)          OVER (ORDER BY d) AS prev_cvr
  FROM daily_kpis
),
flags AS (
  -- events_count
  SELECT
    d AS anomaly_date,
    'overall' AS entity_type,
    NULL AS entity_id,
    'events_count' AS metric_name,
    events_count AS actual_value,
    base_events AS baseline_value,
    CASE WHEN sd_events IS NULL OR sd_events=0 THEN NULL
         ELSE (events_count - base_events) / sd_events END AS z_score,
    CASE WHEN prev_events IS NULL OR prev_events=0 THEN NULL
         ELSE (events_count - prev_events) / prev_events END AS pct_change,
    CASE
      WHEN base_events IS NULL THEN 'N'
      WHEN abs((events_count - base_events) / NULLIF(sd_events,0)) > 3 THEN 'Y'
      WHEN abs((events_count - prev_events) / NULLIF(prev_events,0)) > 0.60 THEN 'Y'
      ELSE 'N'
    END AS anomaly_flag,
    'z>|3| OR day_change>|60%|' AS notes
  FROM kpis_enriched

  UNION ALL

  -- revenue
  SELECT
    d, 'overall', NULL, 'revenue',
    revenue, base_revenue,
    CASE WHEN sd_revenue IS NULL OR sd_revenue=0 THEN NULL
         ELSE (revenue - base_revenue) / sd_revenue END,
    CASE WHEN prev_revenue IS NULL OR prev_revenue=0 THEN NULL
         ELSE (revenue - prev_revenue) / prev_revenue END,
    CASE
      WHEN base_revenue IS NULL THEN 'N'
      WHEN abs((revenue - base_revenue) / NULLIF(sd_revenue,0)) > 3 THEN 'Y'
      WHEN (revenue - prev_revenue) / NULLIF(prev_revenue,0) > 0.60 THEN 'Y'
      WHEN (revenue - prev_revenue) / NULLIF(prev_revenue,0) < -0.40 THEN 'Y'
      ELSE 'N'
    END,
    'z>|3| OR +60% / -40% day_change'
  FROM kpis_enriched

  UNION ALL

  -- cvr
  SELECT
    d, 'overall', NULL, 'cvr',
    cvr, base_cvr,
    CASE WHEN sd_cvr IS NULL OR sd_cvr=0 THEN NULL
         ELSE (cvr - base_cvr) / sd_cvr END,
    CASE WHEN prev_cvr IS NULL OR prev_cvr=0 THEN NULL
         ELSE (cvr - prev_cvr) / prev_cvr END,
    CASE
      WHEN base_cvr IS NULL THEN 'N'
      WHEN abs((cvr - base_cvr) / NULLIF(sd_cvr,0)) > 3 THEN 'Y'
      WHEN abs((cvr - prev_cvr) / NULLIF(prev_cvr,0)) > 0.50 THEN 'Y'
      ELSE 'N'
    END,
    'z>|3| OR day_change>|50%|'
  FROM kpis_enriched
)

INSERT INTO dq.dq_anomalies
SELECT
  anomaly_date, entity_type, entity_id, metric_name,
  actual_value, baseline_value, z_score, pct_change, anomaly_flag,
  notes, now() AS created_at
FROM flags
WHERE anomaly_date = (SELECT run_date FROM params);



----------------------------------------------------------------------------------------------------------------------------------------------------
-- /* ============================================================
--    04_dq_checks.sql (DuckDB)
--    PURPOSE:
--    Run reliability checks for a specific run_date and write
--    results into dq.dq_check_results (replay-friendly).
--    ============================================================ */

-- -- 1) Set your run date here (CHANGE THIS DATE)
-- -- CREATE OR REPLACE TEMP VIEW params AS
-- -- SELECT
-- --   DATE '2019-11-15' AS run_date,
-- --   CAST(DATE '2019-11-15' - INTERVAL '1 day' AS DATE) AS prev_date;-- Expects TEMP VIEW params(run_date, prev_date) to already exist.


-- -- 2) Idempotency: delete existing results for that run_date
-- DELETE FROM dq.dq_check_results
-- WHERE check_date = (SELECT run_date FROM params);

-- -- ------------------------------------------------------------
-- -- CHECK 1: Freshness = do we have events on run_date? (P0)
-- -- ------------------------------------------------------------
-- INSERT INTO dq.dq_check_results
-- SELECT
--   (SELECT run_date FROM params) AS check_date,
--   'freshness_has_events_on_run_date' AS check_name,
--   'core.fact_events' AS table_name,
--   'P0' AS severity,
--   CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
--   COUNT(*)::DOUBLE AS metric_value,
--   '> 0 rows on run_date' AS expected_range,
--   CASE WHEN COUNT(*) > 0 THEN NULL ELSE 'No events found for run_date' END AS failure_details,
--   now() AS created_at
-- FROM core.fact_events
-- WHERE date(event_ts) = (SELECT run_date FROM params);

-- -- ------------------------------------------------------------
-- -- CHECK 2: Volume previous day >= 1000 (P0)
-- -- ------------------------------------------------------------
-- INSERT INTO dq.dq_check_results
-- WITH y AS (
--   SELECT COUNT(*)::DOUBLE AS cnt
--   FROM core.fact_events
--   WHERE date(event_ts) = (SELECT prev_date FROM params)
-- )
-- SELECT
--   (SELECT run_date FROM params),
--   'volume_prev_day_events_min_1000',
--   'core.fact_events',
--   'P0',
--   CASE WHEN cnt >= 1000 THEN 'PASS' ELSE 'FAIL' END,
--   cnt,
--   '>= 1000',
--   CASE WHEN cnt >= 1000 THEN NULL ELSE 'Previous day events below 1000' END,
--   now()
-- FROM y;

-- -- ------------------------------------------------------------
-- -- CHECK 3: Purchase vanish previous day (P0)
-- -- ------------------------------------------------------------
-- INSERT INTO dq.dq_check_results
-- WITH p AS (
--   SELECT COUNT(*)::DOUBLE AS purchases
--   FROM core.fact_events
--   WHERE event_type = 'purchase'
--     AND date(event_ts) = (SELECT prev_date FROM params)
-- )
-- SELECT
--   (SELECT run_date FROM params),
--   'purchase_events_vanish_prev_day',
--   'core.fact_events',
--   'P0',
--   CASE WHEN purchases > 0 THEN 'PASS' ELSE 'FAIL' END,
--   purchases,
--   '> 0',
--   CASE WHEN purchases > 0 THEN NULL ELSE 'No purchases on previous day' END,
--   now()
-- FROM p;

-- -- ------------------------------------------------------------
-- -- CHECK 4: Null session_id % on run_date <= 1% (P1)
-- -- ------------------------------------------------------------
-- INSERT INTO dq.dq_check_results
-- WITH s AS (
--   SELECT
--     100.0 * SUM(CASE WHEN session_id IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0) AS null_pct
--   FROM core.fact_events
--   WHERE date(event_ts) = (SELECT run_date FROM params)
-- )
-- SELECT
--   (SELECT run_date FROM params),
--   'null_rate_session_id_run_date',
--   'core.fact_events',
--   'P1',
--   CASE WHEN null_pct <= 1.0 THEN 'PASS' ELSE 'FAIL' END,
--   null_pct,
--   '<= 1%',
--   CASE WHEN null_pct <= 1.0 THEN NULL ELSE 'session_id null% exceeded 1% on run_date' END,
--   now()
-- FROM s;

-- -- ------------------------------------------------------------
-- -- CHECK 5: Duplicate composite key groups on run_date = 0 (P0)
-- -- ------------------------------------------------------------
-- INSERT INTO dq.dq_check_results
-- WITH dups AS (
--   SELECT COUNT(*)::DOUBLE AS dup_groups
--   FROM (
--     SELECT user_id, session_id, event_ts, event_type, product_id, COUNT(*) c
--     FROM core.fact_events
--     WHERE date(event_ts) = (SELECT run_date FROM params)
--     GROUP BY 1,2,3,4,5
--     HAVING COUNT(*) > 1
--   )
-- )
-- SELECT
--   (SELECT run_date FROM params),
--   'dup_composite_key_groups_run_date',
--   'core.fact_events',
--   'P0',
--   CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
--   dup_groups,
--   '= 0 groups',
--   CASE WHEN dup_groups = 0 THEN NULL ELSE 'Duplicate groups found on run_date' END,
--   now()
-- FROM dups;

/* ============================================================
   04_dq_checks.sql (DuckDB)
   PURPOSE:
   Run reliability checks for the run_date in TEMP VIEW params
   and write results into dq.dq_check_results.
   REQUIREMENT:
   TEMP VIEW params(run_date, prev_date) MUST already exist.
   ============================================================ */

-- Guard: will error immediately if params is missing (good)
SELECT run_date, prev_date FROM params;

-- Idempotency: delete existing results for that run_date
DELETE FROM dq.dq_check_results
WHERE check_date = (SELECT run_date FROM params);

-- 1) Freshness on run_date (P0)
INSERT INTO dq.dq_check_results
INSERT INTO dq.dq_check_results
(check_date, check_name, table_name, severity, status, metric_value, expected_range, failure_details, created_at)
SELECT ...

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

-- 2) Volume previous day >= 1000 (P0)
INSERT INTO dq.dq_check_results
WITH y AS (
  SELECT COUNT(*)::DOUBLE AS cnt
  FROM core.fact_events
  WHERE date(event_ts) = (SELECT prev_date FROM params)
)
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

-- 3) Purchase vanish previous day (P0)
INSERT INTO dq.dq_check_results
WITH p AS (
  SELECT COUNT(*)::DOUBLE AS purchases
  FROM core.fact_events
  WHERE event_type='purchase'
    AND date(event_ts) = (SELECT prev_date FROM params)
)
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

-- 4) Null session_id % on run_date <= 1% (P1)
INSERT INTO dq.dq_check_results
WITH s AS (
  SELECT
    100.0 * SUM(CASE WHEN session_id IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0) AS null_pct
  FROM core.fact_events
  WHERE date(event_ts) = (SELECT run_date FROM params)
)
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
-- CHECK: Purchase count on run_date > 0 (P0)
INSERT INTO dq.dq_check_results
SELECT
  (SELECT run_date FROM params) AS check_date,
  'purchase_events_on_run_date' AS check_name,
  'core.fact_events' AS table_name,
  'P0' AS severity,
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*)::DOUBLE AS metric_value,
  '> 0 purchases' AS expected_range,
  CASE WHEN COUNT(*) > 0 THEN NULL ELSE 'No purchases found on run_date' END AS failure_details,
  now() AS created_at
FROM core.fact_events
WHERE event_type = 'purchase'
  AND date(event_ts) = (SELECT run_date FROM params);
