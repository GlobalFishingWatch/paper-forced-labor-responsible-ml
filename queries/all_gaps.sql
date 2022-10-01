-- !preview conn=DBI::dbConnect(bigrquery::bigquery(),project = "world-fishing-827", use_legacy_sql = FALSE)
-----------------------------------------
-- Query to calculate ssvid/year specific
-- gap statistics for fishing vessels.
-- CHECK DATE RANGE FOR GAPS TABLE
--
-- R.Joo  v2022-09-14
-----------------------------------------
--
--
--SET your date minimum of interest (We don't try to detect disabling events prior to 2017)
CREATE TEMP FUNCTION minimum() AS (TIMESTAMP("2017-01-01"));
--
--SET your date maximum of interest
CREATE TEMP FUNCTION maximum() AS (TIMESTAMP("2021-12-31"));

-----------------------------------------
-- Gaps that may be intentional turning
-- off of AIS for fishing vessels
-----------------------------------------
WITH intentional_gaps AS (
SELECT
    *,
    TIMESTAMP_DIFF(event_end, event_start, HOUR) AS duration,
    ST_GEOGPOINT(CAST(JSON_VALUE(event_info, "$.gap_start_lon") AS FLOAT64), CAST(JSON_VALUE(event_info, "$.gap_start_lat") AS FLOAT64)) as start_point,
    ST_GEOGPOINT(CAST(JSON_VALUE(event_info, "$.gap_end_lon") AS FLOAT64), CAST(JSON_VALUE(event_info, "$.gap_end_lat") AS FLOAT64)) as end_point,
    EXTRACT(YEAR FROM event_start) AS year
    FROM `world-fishing-827.pipe_production_v20201001.proto_published_events_ais_gaps`
    WHERE JSON_VALUE(event_info, "$.intentional_disabling") = 'true'
    AND event_start BETWEEN minimum() AND maximum()
    AND JSON_VALUE(event_info, "$.is_closed") = 'true'
),


-----------------------------------------
-- Extract ssvid and compute gap distance
-----------------------------------------
gaps_ssvid_distance AS (
SELECT
    *,
    JSON_EXTRACT_SCALAR(vessel_info, '$.ssvid') as ssvid,
    ST_DISTANCE(start_point, end_point)/1000 AS distance,
FROM intentional_gaps
LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(event_vessels)) as vessel_info
)


-----------------------------------------
-- Calculate gap stats by SSVID, year
-----------------------------------------
SELECT
  ssvid,
  year,
  COUNT(*) AS gaps_12_hours,
  AVG(start_distance_from_port_km) AS average_off_distance_from_port_km,
  AVG(start_distance_from_shore_km) AS average_off_distance_from_shore_km,
  AVG(duration/24) AS average_gap_days,
  AVG(distance) AS average_gap_km,
FROM
  gaps_ssvid_distance
GROUP BY
  ssvid,
  year
