-- !preview conn=DBI::dbConnect(bigrquery::bigquery(),project = "world-fishing-827", use_legacy_sql = FALSE)
#standardSQL
-----------------------------------------
-- Query to get positions for all fishing
-- vessels along with position metadata.
-- NOTE: Expensive query run over
-- all-time. ONLY RUN WHEN NECESSARY.
--
-- N.Miller  v2020-01-28
-- R.Joo  v2021-03-16
--
--
--SET your date minimum of interest
CREATE TEMP FUNCTION minimum() AS (TIMESTAMP("2012-01-01"));
--
--SET your date maximum of interest
CREATE TEMP FUNCTION maximum() AS (TIMESTAMP("2021-12-31"));
-----------------------------------------
--
--
-----------------------------------------
-- Load list of fishing vessels by year
-- including fishing vessel class
-----------------------------------------
WITH fishing_vessels AS (
  SELECT
  ssvid,
  year,
  best_vessel_class
  FROM
  `world-fishing-827.gfw_research.fishing_vessels_ssvid_v20220801`
  --`world-fishing-827.gfw_research.vi_ssvid_byyear_v20210301`
  --WHERE udfs.is_fishing(best.best_vessel_class)

),

-----------------------------------------
-- Pull all fishing vessel positions
-- along with metadata for each position
-- while removing noisy positions.
-----------------------------------------
vessel_positions AS (
  SELECT
  ssvid,
  timestamp,
  seg_id,
  hours,
  meters_to_prev,
  lat,
  lon,
  nnet_score,
  night_loitering,
  distance_from_shore_m,
  distance_from_port_m,
  speed_knots,
  regions.eez AS eez,
  EXTRACT(year FROM _partitiontime) as year
FROM
  `world-fishing-827.pipe_production_v20201001.research_messages`
WHERE
  is_fishing_vessel IS TRUE AND
  _PARTITIONTIME BETWEEN minimum() AND maximum() AND
  distance_from_port_m > 1000 AND -- arbitrary limit to make sure that the vessel is at sea
  seg_id IN(
  SELECT
    seg_id
  FROM
    `world-fishing-827.pipe_production_v20201001.research_segs`
  WHERE
    good_seg IS TRUE AND
    overlapping_and_short IS FALSE)
)
-----------------------------------------
-- Join positions onto fishing vessels to
-- assign vessel class. If vessel is squid
-- jigger set nnet_score (fishing score) to 1
-- when night_loitering = 1. For vessels
-- that are not squidjiggers, use a nnet_score
-- greater than 0.5 cutoff, otherwise assign
-- nnet_score to 0
-----------------------------------------
SELECT
 ssvid,
  timestamp,
  seg_id,
  hours,
  meters_to_prev,
  lat,
  lon,
  night_loitering,
  distance_from_shore_m,
  distance_from_port_m,
  speed_knots,
  eez,
  CASE
    WHEN best_vessel_class = 'squid_jigger' AND night_loitering = 1 THEN 1
    WHEN nnet_score > 0.5 AND NOT best_vessel_class = 'squid_jigger' THEN 1
    ELSE 0
  END
  AS nnet_score
  FROM
  vessel_positions
  JOIN
  fishing_vessels
  USING (ssvid, year)
