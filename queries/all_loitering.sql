-- !preview conn=DBI::dbConnect(bigrquery::bigquery(),project = "world-fishing-827", use_legacy_sql = FALSE)
#standardSQL
-----------------------------------------
-- Query to pull loitering events for all
-- vessels (locations where vessels travel
-- at speeds of less than 2 knots for a
-- period of time). NOTE: This table was
-- created with v20190502 of the pipeline
-- and also only has data up to 2020-12-12
--
-- R.Joo  v2021-03-16

--SET your date minimum of interest
CREATE TEMP FUNCTION minimum() AS (TIMESTAMP("2012-01-01"));
--
--SET your date maximum of interest
CREATE TEMP FUNCTION maximum() AS (TIMESTAMP("2021-12-31"));
-----------------------------------------
--
-----------------------------------------
-- Get loitering events during specified
-- time period. NOTE: Original loitering
-- duration was 1 hour. This seemed pretty
-- short, so I have changed it to our more
-- standard 4 hours. We can revisit this.
-----------------------------------------
WITH
  base AS(
  SELECT
    ssvid,
    EXTRACT(YEAR
    FROM
      loitering_start_timestamp) year,
    TIMESTAMP_DIFF(loitering_end_timestamp,loitering_start_timestamp,SECOND)/3600 AS loitering_duration_hours
  FROM
    `world-fishing-827.pipe_production_v20201001.loitering`
  WHERE
  loitering_start_timestamp <= maximum()
  AND loitering_end_timestamp >= minimum()
  AND seg_id IN (SELECT
        seg_id
        FROM
        `world-fishing-827.pipe_production_v20201001.research_segs`
        WHERE
        good_seg IS TRUE
  AND overlapping_and_short IS FALSE)
  AND avg_distance_from_shore_nm > 10
  AND loitering_hours > 4 )
-----------------------------------------
-- Calculate ssvid level yearly summaries
-- of number of loitering events and
-- duration
-----------------------------------------
SELECT
  ssvid,
  year,
  COUNT(*) AS number_loitering_events,
  AVG(loitering_duration_hours) AS average_loitering_duration_hours
FROM
  base
GROUP BY
  ssvid,
  year
