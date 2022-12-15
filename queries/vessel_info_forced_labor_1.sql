#standardSQL
-----------------------------------------
-- vessel_info_forced_labor_1
-- First part of the query
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
-----------------------------------------
-- Add vessel info to each forced
-- labor vessel using MMSI
-----------------------------------------
  joined_ssvid AS(
  SELECT
    *
  FROM
    vessels_spreadsheet
  JOIN
    vessel_info
  ON (
    vessels_spreadsheet.fl_ssvid = vessel_info.ssvid
    --AND vessels_spreadsheet.year_end = vessel_info.follow_year
    )
  ),

-----------------------------------------
-- Add vessel info to each forced
-- labor vessel using imo if there is no
-- ssvid
-----------------------------------------
  joined_imo AS(
  SELECT
    *
  FROM
    vessels_spreadsheet
  JOIN
    vessel_info
  ON (
    (vessels_spreadsheet.fl_ssvid IS NULL
    OR
    vessels_spreadsheet.fl_ssvid != vessel_info.ssvid)
    AND vessels_spreadsheet.fl_imo = vessel_info.imo
    --AND vessels_spreadsheet.year_end = vessel_info.follow_year
    )
  WHERE fl_event_id NOT IN (SELECT fl_event_id FROM joined_ssvid)
  ),

-----------------------------------------
-- Add vessel info to each forced
-- labor vessel using callsign if there is no
-- ssvid or imo
-----------------------------------------
  joined_callsign AS(
  SELECT
    *
  FROM
    vessels_spreadsheet
  JOIN
    vessel_info
  ON (
    (vessels_spreadsheet.fl_ssvid IS NULL
    OR
    vessels_spreadsheet.fl_ssvid != vessel_info.ssvid)
    AND
    (vessels_spreadsheet.fl_imo IS NULL
    OR
    vessels_spreadsheet.fl_imo != vessel_info.imo)
    AND vessels_spreadsheet.fl_callsign  = vessel_info.callsign
    --AND vessels_spreadsheet.year_end = vessel_info.follow_year
    )
  WHERE fl_event_id NOT IN (SELECT fl_event_id FROM joined_ssvid) AND
      fl_event_id NOT IN (SELECT fl_event_id FROM joined_imo)
  )


-----------------------------------------
-- Combine different matching methods
-----------------------------------------
SELECT
*
FROM (
  SELECT
  *
  FROM
  joined_ssvid

  UNION ALL

  SELECT
  *
  FROM
  joined_imo

  UNION ALL

  SELECT
  *
  FROM
  joined_callsign


  )
