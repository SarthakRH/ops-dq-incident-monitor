/* ============================================================
   02_replay.sql  (DuckDB)
   ------------------------------------------------------------
   PURPOSE (what this file does)
   This simulates a real "daily ingestion" pipeline.

   - core.fact_events_all = full history (never changes)
   - core.fact_events     = current loaded data (grows day-by-day)

   You will run this file repeatedly, changing RUN_DATE
   to ingest the next day (or a range of days).

   WHY we do this:
   - Monitoring checks (freshness/volume/anomalies) only make sense
     if data arrives in increments like a real pipeline.
   ============================================================ */


-- -------------------------------
-- 0) OPTIONAL: Reset replay table
-- -------------------------------
-- Use this when you want to restart the simulation from scratch.
-- (Commented out by default so you don't accidentally wipe data.)

-- TRUNCATE TABLE core.fact_events;


-- -----------------------------------------
-- 1) Choose the date you want to "ingest"
-- -----------------------------------------
-- Update this date every time you want to load a new day.
-- Pick dates that exist inside core.fact_events_all.

WITH params AS (
  SELECT DATE '2019-11-01' AS run_date   -- <-- CHANGE THIS DATE EACH RUN
)

-- ---------------------------------------------------------
-- 2) Insert that day's rows from full history into replay
-- ---------------------------------------------------------
INSERT INTO core.fact_events
SELECT f.*
FROM core.fact_events_all f
JOIN params p
  ON date(f.event_ts) = p.run_date;


-- ---------------------------------------------------------
-- 3) Safety check: prevent accidental double-ingestion
-- ---------------------------------------------------------
-- If you run the same run_date twice, you'll duplicate rows.
-- This block gives you a warning-style output you can run manually
-- AFTER ingestion to confirm whether duplicates were created.

-- COPY-PASTE to run manually:
-- WITH params AS (SELECT DATE '2019-11-05' AS run_date)
-- SELECT
--   p.run_date,
--   COUNT(*) AS rows_loaded_for_day
-- FROM core.fact_events f
-- JOIN params p ON date(f.event_ts) = p.run_date
-- GROUP BY 1;

-- ---------------------------------------------------------
-- 4) Optional: show how far replay has progressed
-- ---------------------------------------------------------
-- COPY-PASTE to run manually:
-- SELECT
--   MIN(date(event_ts)) AS min_loaded_date,
--   MAX(date(event_ts)) AS max_loaded_date,
--   COUNT(*)            AS total_rows_loaded
-- FROM core.fact_events;
