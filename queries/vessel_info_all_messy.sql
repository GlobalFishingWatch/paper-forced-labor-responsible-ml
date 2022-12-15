-- !preview conn=DBI::dbConnect(bigrquery::bigquery(),project = "world-fishing-827", use_legacy_sql = FALSE)
#standardSQL
-----------------------------------------
-- Query to generate vessel info table
-- for all vessels with FOC and IUU data
-- as well as standard vessel information
--
-- N. Miller v.2020-01-28
-- R. Joo v. 2021-03-16
-----------------------------------------
--
--
-----------------------------------------
-- Get list of FOC vessels
-----------------------------------------
  WITH
  foc AS(
  SELECT
  iso3 AS flag,
  TRUE AS foc
  FROM
  `world-fishing-827.gfw_research.flags_of_convenience_v20211013`
  ),
-----------------------------------------
-- Get list of IUU vessels
-----------------------------------------
  iuu AS(
  SELECT
    ssvid,
    TRUE AS iuu
  FROM
    `prj_forced_labor.all_iuu`),
-----------------------------------------
-- Not using this because we're missing some vessels
-- Get list of GFW fishing vessels
-----------------------------------------
--  fishing_vessels AS(
--  SELECT
--    * EXCEPT(best_vessel_class, best_flag),
--    best_vessel_class as gear,
--    best_flag as flag
--  FROM
--    `gfw_research.fishing_vessels_ssvid_v20210301`),
-----------------------------------------
-- Get AIS transmitter type for each ssvid
-- by year
-----------------------------------------
  master_ais AS(
  SELECT
    ssvid,
    year,
    (CASE
        WHEN ais_type.value IN ('AIS.1',  'AIS.2',  'AIS.3') THEN 'A'
        WHEN ais_type.value IN ('AIS.18',
        'AIS.19') THEN 'B'
        ELSE NULL END) AS type,
    ais_type.count AS count
  FROM
    `world-fishing-827.gfw_research.vi_ssvid_byyear_v20220801`
  CROSS JOIN
    UNNEST(activity.position_type) AS ais_type),
-----------------------------------------
-- For each ssvid and year, identify
-- the total positions from each transmitter
-- type (Class A vs Class B). Rank them by
-- total count
-----------------------------------------
  grouped_ais AS(
  SELECT
    ssvid,
    year,
    type,
    SUM(count) AS count,
    ROW_NUMBER() OVER(PARTITION BY ssvid, year ORDER BY SUM(count) DESC) AS rank
  FROM
    master_ais
  GROUP BY
    ssvid,
    year,
    type),
-----------------------------------------
-- Assign each ssvid the most common
-- transmitter class by year (rank 1)
-----------------------------------------
  final_ais AS(
  SELECT
    ssvid,
    year,
    type AS ais_type
  FROM
    grouped_ais
  WHERE
    rank = 1),
-----------------------------------------
-- Pull additional vessel info data and
-- and filter to GFW list of active,
-- non offsetting/spoofing fishing vessels
-- NOTE (Tyler): Offsetting/spoofing vessels are some of the
-- sketchier vessels and ommitting them is a bit odd IMO
-----------------------------------------
  vessel_info AS(
  SELECT
    year,
    ssvid,
    ais_identity.n_shipname_mostcommon.value AS shipname,
    registry_info.best_known_shipname AS registry_shipname,
    registry_info.best_known_imo AS imo,
    ais_identity.n_imo_mostcommon.value AS ais_imo,
    registry_info.best_known_callsign AS callsign,
    ais_identity.n_callsign_mostcommon.value AS ais_callsign,
    best.best_vessel_class AS gear,
    best.best_flag AS flag,
    best.best_engine_power_kw AS engine_power_kw,
    best.best_tonnage_gt AS tonnage_gt,
    best.best_length_m AS length_m,
    -- best.best_crew_size AS crew_size, --Tim said that this is not well predicted
    activity.positions AS positions,
    -- activity.offsetting AS offsetting, --don't need it anymore
    activity.overlap_hours_multinames AS overlap_hours_multinames,
    on_fishing_list_best
  FROM
    `world-fishing-827.gfw_research.vi_ssvid_byyear_v20220801`
  --JOIN -- should it be another type of join?
  --  fishing_vessels
  --USING(ssvid, year)),
  ),
-----------------------------------------
-- Generate a table of ssvids by year
--  with associated vessel info, including
-- AIS class (A or B), if they are foc
-----------------------------------------
final AS(
SELECT
  *
FROM
  vessel_info
LEFT JOIN
  final_ais USING(ssvid,
    year)
LEFT JOIN
foc USING(flag)
LEFT JOIN
iuu USING(ssvid)
)
-----------------------------------------
-- Generate final table with foc and iuu
-- as logicals. Remove unwanted fields.
-- Removing non fishing vessels.
-----------------------------------------

-- CREATE TABLE `world-fishing-827.prj_forced_labor.vessel_info_all.sql`
SELECT
*
EXCEPT(foc,iuu),
IF(foc,TRUE,FALSE) AS foc,
IF(iuu,TRUE,FALSE) AS iuu
FROM
final
WHERE -- make sure that only fishing vessels would stay on the list -- ask Tyler to check this
udfs.is_fishing(gear)
--on_fishing_list_best IS TRUE
--OR gear = 'fishing'
