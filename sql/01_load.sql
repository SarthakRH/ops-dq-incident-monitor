CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS core;

CREATE OR REPLACE TABLE stage.events_raw AS
SELECT *
FROM read_csv_auto(
  'data/raw/2019-Nov.csv',      --creating a staging table with all the csv data into it 
  header = true,
  sample_size = -1             -- read full file for better type inference
);
CREATE OR REPLACE TABLE core.fact_events_all AS
SELECT
  /* ---- IDs (cast to VARCHAR so joins are consistent) ---- */
  CAST(user_id AS VARCHAR)                      AS user_id,
  CAST(user_session AS VARCHAR)                 AS session_id,
  CAST(product_id AS VARCHAR)                   AS product_id,
  CAST(category_id AS VARCHAR)                  AS category_id,

  /* ---- optional descriptive columns ---- */
  NULLIF(TRIM(CAST(category_code AS VARCHAR)), '') AS category_code,
  NULLIF(TRIM(CAST(brand AS VARCHAR)), '')         AS brand,

  /* ---- event type (normalize to lowercase) ---- */
  LOWER(NULLIF(TRIM(CAST(event_type AS VARCHAR)), '')) AS event_type,

  /* ---- timestamp parsing ----
     Your data sometimes contains " UTC" at the end.
     We remove that if present and TRY_CAST safely.
  */
  CASE
    WHEN CAST(event_time AS VARCHAR) LIKE '%UTC'
      THEN TRY_CAST(REPLACE(CAST(event_time AS VARCHAR), ' UTC', '') AS TIMESTAMP)
    ELSE TRY_CAST(CAST(event_time AS VARCHAR) AS TIMESTAMP)
  END AS event_ts,

  /* ---- price as DOUBLE (safe cast) ---- */
  TRY_CAST(price AS DOUBLE) AS price,

  /* ---- event_id (surrogate key) ----
     Many raw datasets don't have a unique event_id.
     We create a deterministic hash from key fields.
     This helps later for duplicate checks / auditing.
  */
  MD5(
    CONCAT_WS('|',
      CAST(user_id AS VARCHAR),
      CAST(user_session AS VARCHAR),
      CAST(event_time AS VARCHAR),
      CAST(event_type AS VARCHAR),
      CAST(product_id AS VARCHAR),
      CAST(price AS VARCHAR)
    )
  ) AS event_id

FROM stage.events_raw
WHERE
  /* Basic sanity filter: keep only rows with a parseable timestamp */
  CASE
    WHEN CAST(event_time AS VARCHAR) LIKE '%UTC'
      THEN TRY_CAST(REPLACE(CAST(event_time AS VARCHAR), ' UTC', '') AS TIMESTAMP)
    ELSE TRY_CAST(CAST(event_time AS VARCHAR) AS TIMESTAMP)
  END IS NOT NULL
;
