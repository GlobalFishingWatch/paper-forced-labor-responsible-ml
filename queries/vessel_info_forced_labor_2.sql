#standardSQL
-----------------------------------------
-- vessel_info_forced_labor_2
-- Second part of the query
-- Query to create a dataset with
-- vessel info for each ssvid of offenders,
-- possible offenders and non offenders,
-- as well as
-- details of the from the forced
-- labor dataset
--
-- N.Miller  v2020-01-28
-- R.Joo  v2021-03-16
-----------------------------------------
--
--
-----------------------------------------
-- Load table with vessel info data for
-- all vessels
-----------------------------------------
  WITH vessel_info AS(
  SELECT
    *
    FROM
    `prj_forced_labor.vessel_info_all_messy`),
-----------------------------------------
-- Load vessels/instances from forced
-- labor database
-----------------------------------------
  vessels_spreadsheet AS(
  SELECT
    * EXCEPT(mmsi, imo, callsign, vessel_name, year_focus),
    CAST(mmsi AS STRING) AS fl_ssvid,
    imo AS fl_imo,
    callsign AS fl_callsign,
    vessel_name AS fl_shipname,
    ROW_NUMBER() OVER() AS fl_event_id,
    CAST (year_focus AS INT64) AS year_focus
  FROM
    `prj_forced_labor.forced_labor`),

joined_part_1 AS(
SELECT *
FROM
`world-fishing-827.prj_forced_labor.vessel_info_forced_labor_1`
    ),

-----------------------------------------
-- Add vessel info to each forced labor
-- vessel using normalized shipname when
-- forced labor ssvid is NULL when fl_event_id
-- is not already matched using SSVID (ssvid
-- is considered the 'best' match), or imo
-- or callsign
-- Only for known or possible offenders
-----------------------------------------
joined_names_1 AS(
  SELECT
    *
  FROM
    vessels_spreadsheet
  JOIN
    vessel_info
  ON (
vessels_spreadsheet.fl_ssvid IS NULL
      --AND vessels_spreadsheet.year_end = vessel_info.follow_year
      AND vessels_spreadsheet.fl_shipname = vessel_info.shipname
      AND vessels_spreadsheet.common_names = 0)
WHERE fl_event_id NOT IN (SELECT fl_event_id FROM joined_part_1) AND
      known_non_offender = 0
  ),

-----------------------------------------
-- Add vessel info to each forced labor
-- vessel using normalized shipname when
-- forced labor ssvids do not match.. as
-- long at the fl_event_id is not already
-- matched
-----------------------------------------
joined_names_2 AS(
  SELECT
    *
  FROM
    vessels_spreadsheet
  JOIN
    vessel_info
  ON (
      vessels_spreadsheet.fl_ssvid != vessel_info.ssvid
      --AND vessels_spreadsheet.year_end = vessel_info.follow_year
      AND vessels_spreadsheet.fl_shipname = vessel_info.shipname
      AND vessels_spreadsheet.common_names = 0)
WHERE fl_event_id NOT IN (SELECT fl_event_id FROM joined_part_1) AND
      fl_event_id NOT IN (SELECT fl_event_id FROM joined_names_1) AND
      known_non_offender = 0
  ),

------------------------------------------------
-- Same as joined_names_1 and 2 but with registry name
------------------------------------------------

joined_names_3 AS(
  SELECT
    *
  FROM
    vessels_spreadsheet
  JOIN
    vessel_info
  ON (
      vessels_spreadsheet.fl_ssvid IS NULL
      --AND vessels_spreadsheet.year_end = vessel_info.follow_year
      AND vessels_spreadsheet.fl_shipname = vessel_info.registry_shipname
      AND vessels_spreadsheet.common_names = 0)
WHERE fl_event_id NOT IN (SELECT fl_event_id FROM joined_part_1) AND
      fl_event_id NOT IN (SELECT fl_event_id FROM joined_names_1) AND
      fl_event_id NOT IN (SELECT fl_event_id FROM joined_names_2) AND
      known_non_offender = 0
  ),

joined_names_4 AS(
  SELECT
    *
  FROM
    vessels_spreadsheet
  JOIN
    vessel_info
  ON (
      vessels_spreadsheet.fl_ssvid != vessel_info.ssvid
      --AND vessels_spreadsheet.year_end = vessel_info.follow_year
      AND vessels_spreadsheet.fl_shipname = vessel_info.registry_shipname
      AND vessels_spreadsheet.common_names = 0)
WHERE fl_event_id NOT IN (SELECT fl_event_id FROM joined_part_1) AND
      fl_event_id NOT IN (SELECT fl_event_id FROM joined_names_1) AND
      fl_event_id NOT IN (SELECT fl_event_id FROM joined_names_2) AND
      fl_event_id NOT IN (SELECT fl_event_id FROM joined_names_3) AND
      known_non_offender = 0
  ),

-----------------------------------------
-- Combine different matching methods
-----------------------------------------
combined AS (SELECT
*
FROM (

  SELECT
  *
  FROM
  joined_part_1

  UNION ALL

  SELECT
  *
  FROM
  joined_names_1

  UNION ALL

  SELECT
  *
  FROM
  joined_names_2

  UNION ALL

  SELECT
  *
  FROM
  joined_names_3

  UNION ALL

  SELECT
  *
  FROM
  joined_names_4

  )
),


-----------------------------------------
-- Add a index (rownumber) using
-- force_labor_id, mmsi, and year.
-----------------------------------------
rownum AS (
  SELECT
    *, --,
    ROW_NUMBER() OVER (PARTITION BY ssvid,year) AS rownumber
  FROM
    combined
  ORDER BY
    fl_event_id,
    year
)

-----------------------------------------
-- Only select vessels that are ranked 1.
-- A rank different than one could happen
-- when there are two names of vessels
-- and a match for both, so that the same
-- forced labor event would have a match
-- with two different AIS vessel-year info.
-- Also create fields to identify if
-- the date from the forced labor database
-- (MMSI, IMO, or shipname) matched the
-- data from the vessel info table as
-- logicals (TRUE/FALSE)
-- Also created past_ais_year for AIS data
-- from the year of the event or before
-- and event_ais_year for AIS data from
-- the years during the event
-----------------------------------------

SELECT
  *
  EXCEPT(rownumber, num_years),
  CAST (num_years AS INT64) AS num_years,
  (CASE
    WHEN year_focus >= year THEN 1
    ELSE 0 END) AS past_ais_year,
  (CASE
    WHEN year_focus >= year AND year_focus - year < num_years THEN 1
    ELSE 0 END) AS event_ais_year
FROM
  rownum
WHERE
  rownumber = 1 --AND
  --fl_ssvid = '412329687'
