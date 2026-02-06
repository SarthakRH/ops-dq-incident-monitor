-- /* ============================================================
--    05_anomalies.sql (DuckDB) - Replay-friendly
--    REQUIRES: TEMP VIEW params(run_date, prev_date) already exists
--    ============================================================ */

-- -- Guard: fail fast if params missing
-- SELECT run_date FROM params;

-- -- Idempotency
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
--     CASE WHEN events_count=0 THEN NULL ELSE purchases / events_count END AS cvr,

--     avg(events_count) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS base_events,
--     stddev_samp(events_count) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS sd_events,

--     avg(revenue) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS base_revenue,
--     stddev_samp(revenue) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS sd_revenue,

--     avg(cvr) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS base_cvr,
--     stddev_samp(cvr) OVER (ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS sd_cvr,

--     lag(events_count) OVER (ORDER BY d) AS prev_events,
--     lag(revenue)      OVER (ORDER BY d) AS prev_revenue,
--     lag(cvr)          OVER (ORDER BY d) AS prev_cvr
--   FROM daily_kpis
-- ),
-- flags AS (
--   -- events_count
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

--   -- revenue
--   SELECT
--     d, 'overall', NULL, 'revenue',
--     revenue, base_revenue,
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

--   -- cvr
--   SELECT
--     d, 'overall', NULL, 'cvr',
--     cvr, base_cvr,
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
--   anomaly_date, entity_type, entity_id, metric_name,
--   actual_value, baseline_value, z_score, pct_change, anomaly_flag,
--   notes, now() AS created_at
-- FROM flags
-- WHERE anomaly_date = (SELECT run_date FROM params);


-- REQUIREMENT: TEMP VIEW params(run_date, prev_date) already exists

SELECT run_date, prev_date FROM params;

DELETE FROM dq.dq_anomalies
WHERE anomaly_date = (SELECT run_date FROM params);

WITH daily_kpis AS (
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
    d, events_count, purchases, revenue,
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
  SELECT
    d AS anomaly_date, 'overall' AS entity_type, NULL AS entity_id,
    'events_count' AS metric_name,
    events_count AS actual_value, base_events AS baseline_value,
    CASE WHEN sd_events IS NULL OR sd_events=0 THEN NULL ELSE (events_count - base_events)/sd_events END AS z_score,
    CASE WHEN prev_events IS NULL OR prev_events=0 THEN NULL ELSE (events_count - prev_events)/prev_events END AS pct_change,
    CASE
      WHEN base_events IS NULL THEN 'N'
      WHEN abs((events_count - base_events)/NULLIF(sd_events,0)) > 3 THEN 'Y'
      WHEN abs((events_count - prev_events)/NULLIF(prev_events,0)) > 0.60 THEN 'Y'
      ELSE 'N'
    END AS anomaly_flag,
    'z>|3| OR day_change>|60%|' AS notes
  FROM kpis_enriched

  UNION ALL
  SELECT
    d, 'overall', NULL, 'revenue',
    revenue, base_revenue,
    CASE WHEN sd_revenue IS NULL OR sd_revenue=0 THEN NULL ELSE (revenue - base_revenue)/sd_revenue END,
    CASE WHEN prev_revenue IS NULL OR prev_revenue=0 THEN NULL ELSE (revenue - prev_revenue)/prev_revenue END,
    CASE
      WHEN base_revenue IS NULL THEN 'N'
      WHEN abs((revenue - base_revenue)/NULLIF(sd_revenue,0)) > 3 THEN 'Y'
      WHEN (revenue - prev_revenue)/NULLIF(prev_revenue,0) > 0.60 THEN 'Y'
      WHEN (revenue - prev_revenue)/NULLIF(prev_revenue,0) < -0.40 THEN 'Y'
      ELSE 'N'
    END,
    'z>|3| OR +60% / -40% day_change'
  FROM kpis_enriched

  UNION ALL
  SELECT
    d, 'overall', NULL, 'cvr',
    cvr, base_cvr,
    CASE WHEN sd_cvr IS NULL OR sd_cvr=0 THEN NULL ELSE (cvr - base_cvr)/sd_cvr END,
    CASE WHEN prev_cvr IS NULL OR prev_cvr=0 THEN NULL ELSE (cvr - prev_cvr)/prev_cvr END,
    CASE
      WHEN base_cvr IS NULL THEN 'N'
      WHEN abs((cvr - base_cvr)/NULLIF(sd_cvr,0)) > 3 THEN 'Y'
      WHEN abs((cvr - prev_cvr)/NULLIF(prev_cvr,0)) > 0.50 THEN 'Y'
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
