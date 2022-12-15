-- !preview conn=DBI::dbConnect(bigrquery::bigquery(),project = "world-fishing-827", use_legacy_sql = FALSE)
#standardSQL
-----------------------------------------
-- Query to generate fishing effort, time
-- at sea, and distance from shore/port metrics
-- per ssvid per year, includes fishing in
-- foreign EEZs. Data for all vessels.
--
-- N.Miller  v2020-01-29
-- R.Joo  v2021-03-16
-----------------------------------------
--
--
-----------------------------------------
-- Calculating fishing and
-- identify location, including EEZs that
-- vessels operate in per day
-----------------------------------------
  WITH base AS(
  SELECT
    ssvid,
    timestamp,
    EXTRACT(YEAR FROM timestamp) AS year,
    seg_id,
    hours,
    (CASE
        WHEN nnet_score = 1
             AND NOT (distance_from_shore_m < 1000
             AND speed_knots < 1) THEN hours
        ELSE 0 END) AS fishing_hours,
    EXTRACT(date FROM timestamp) AS date,
    meters_to_prev,
    distance_from_shore_m,
    distance_from_port_m,
    ARRAY_LENGTH(eez) AS num_eezs,
    IF(ARRAY_LENGTH(eez)=0,NULL,eez[ordinal(1)]) AS eez_id
  FROM
    {`all_vessel_positions_table`}),
-----------------------------------------
-- Add EEZ iso3
-- NOTE: this was missing from the original
-- query.
-----------------------------------------
base_eez_iso3 AS (SELECT
* EXCEPT (eez_id)
FROM
base
LEFT JOIN
(SELECT
SAFE_CAST(eez_id AS STRING) AS eez_id,
territory1_iso3 AS eez_iso3
FROM
`world-fishing-827.gfw_research.eez_info`)
USING (eez_id)
),
-----------------------------------------
-- Add vessel vessel flag
-----------------------------------------
  base_flag AS(
  SELECT
    *
  FROM
    base_eez_iso3
  JOIN (
    SELECT
      ssvid,
      year,
      flag
    FROM
      {`vessel_info_all_table`})
      USING(ssvid,year)),
-----------------------------------------
-- For vessels that fished in one EEZ,
-- identify the number of hours fished in
-- foreign EEZ. Label fishing hours not
-- in an EEZ as high seas fishing. If
-- num_eezs > 1, we currently exclude that
-- day as the vessel may be on a eez boundary
-- and it may not be clear which EEZ the vessel
-- is operating it. benefit of the doubt,
-- we assume its not fishing in foreign EEZ.
-- NOTE: we might be able to account of the
-- amount of fishing that day in different regions
-- and do this better... not sure it will
-- matter though
-----------------------------------------
  base_eez AS(
  SELECT
    *,
    (CASE
        WHEN (NOT eez_iso3 IS NULL
              AND NOT eez_iso3 = flag
              AND fishing_hours > 0
              AND num_eezs = 1) THEN fishing_hours
        ELSE 0 END) AS fishing_hours_foreign_eez,
    (CASE
        WHEN (eez_iso3 IS NULL
              AND fishing_hours > 0) THEN fishing_hours
        ELSE 0 END) AS fishing_hours_high_seas
  FROM
    base_flag),
-----------------------------------------
-- For each ssvid and day generate a number
-- of metrics regarding time at sea, distance
-- from shore/port, and foreign EEZ or high
-- seas fishing.
-----------------------------------------
  daily AS(
  SELECT
    ssvid,
    date,
    year,
    COUNT(*) AS position_messages,
    SUM(hours) AS hours,
    SUM(fishing_hours) AS fishing_hours,
    (CASE
        WHEN SUM(fishing_hours) > 0 THEN SUM(fishing_hours)
        ELSE NULL END) AS daily_fishing_hours,
    SUM(fishing_hours_foreign_eez) AS fishing_hours_foreign_eez,
    SUM(fishing_hours_high_seas)AS fishing_hours_high_seas,
    SUM(meters_to_prev)/1000 AS distance_traveled_km,
    MAX(distance_from_shore_m)/1000 AS max_distance_from_shore_km,
    MAX(distance_from_port_m)/1000 AS max_distance_from_port_km
  FROM
    base_eez
  GROUP BY
    ssvid,
    year,
    date)
-----------------------------------------
-- Summarize metrics by ssvid and year
-----------------------------------------
SELECT
  ssvid,
  year,
  SUM(position_messages) AS position_messages,
  SUM(hours) AS hours,
  SUM(fishing_hours) AS fishing_hours,
  AVG(daily_fishing_hours) AS average_daily_fishing_hours,
  SUM(fishing_hours_foreign_eez) AS fishing_hours_foreign_eez,
  SUM(fishing_hours_high_seas) AS fishing_hours_high_seas,
  SUM(distance_traveled_km) AS distance_traveled_km,
  MAX(max_distance_from_shore_km) AS max_distance_from_shore_km,
  MAX(max_distance_from_port_km) AS max_distance_from_port_km
FROM
daily
GROUP BY
  ssvid,
  year
