-- !preview conn=DBI::dbConnect(bigrquery::bigquery(),project = "world-fishing-827", use_legacy_sql = FALSE)
#standardSQL
-----------------------------------------
-- Matching Encounter data to SSVID values
-- based off Hannah Linder, September 18, 2019
--
-- This query gets encounter data for specific
-- ssvids
--

-- R.Joo  v2021-03-16

--SET your date minimum of interest
CREATE TEMP FUNCTION minimum() AS (TIMESTAMP("2012-01-01"));
--
--SET your date maximum of interest
CREATE TEMP FUNCTION maximum() AS (TIMESTAMP("2021-12-31"));
-----------------------------------------
WITH
  encounters AS (
  SELECT
    event_id,
    vessel_id,
    CAST (event_start AS DATE) AS event_date,
    EXTRACT(YEAR FROM event_start) AS year,
    TIMESTAMP_DIFF(event_end,event_start,HOUR) AS event_duration_hours
  FROM
    `world-fishing-827.pipe_production_v20201001.published_events_encounters`
  WHERE
    lat_mean < 90
    AND lat_mean > -90
    AND lon_mean < 180
    AND lon_mean > -180
    AND event_start BETWEEN minimum() AND maximum()),
-----------------------------------------
-- map of ssvid to vessel_id
-----------------------------------------
  ssvid_map AS (
  SELECT
    vessel_id,
    ssvid
  FROM
    `world-fishing-827.pipe_production_v20201001.vessel_info`),
-----------------------------------------
-- Adding SSVID to the encounters
-----------------------------------------
  encounter_ssvid AS (
  SELECT
    * EXCEPT(vessel_id)
  FROM (
    SELECT
      *
    FROM
      encounters) a
  JOIN (
    SELECT
      *
    FROM
      ssvid_map) b
  ON
    a.vessel_id = b.vessel_id),
-----------------------------------------
-- list of forced labor vessels
-----------------------------------------
  forced_labor_table AS(
  SELECT
    ssvid,
    year,
    1 forced_labor
  FROM
    `prj_forced_labor.vessel_info_forced_labor_2`
  WHERE
    known_offender = 1 -- only known offenders here
    AND
    event_ais_year = 1 -- where the AIS data matches the years of the event
    ),

-----------------------------------------
-- list of IUU vessels
-----------------------------------------
  iuu_table AS(
  SELECT
    ssvid,
    1 iuu
  FROM
    `prj_forced_labor.all_iuu`),
-----------------------------------------
-- Join to create complete encounter with
-- second MMSI (two vessels are involved
-- in an encounter) and then add IUU and
-- forced labor flags
-----------------------------------------
  joined_table AS(
  SELECT
*
FROM
encounter_ssvid
  LEFT JOIN
    iuu_table
  USING
    (ssvid)
  LEFT JOIN
    forced_labor_table
  USING
    (ssvid,
      year)
      )
-----------------------------------------
-- Summarize number of events by SSVID/year
-----------------------------------------
SELECT
  ssvid,
  year,
  COUNT(*) number_encounters,
  IF(SUM(iuu) IS NULL,0,SUM(iuu)) AS number_iuu_encounters,
  IF(SUM(forced_labor) IS NULL,0,SUM(forced_labor)) AS number_forced_labor_encounters,
  AVG(event_duration_hours) AS average_encounter_duration_hours
FROM
  joined_table
GROUP BY
  ssvid,
  year
