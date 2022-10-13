  #standardSQL
  -----------------------------------------
  -- Query to get voyages by vessel/year along
  -- with info on POCs, achorages
  --
  -- G. McDonald, August 16, 2022
  -- Adapted from R. Joo's prj-forced-labor/blob/main/queries/all_port_visits.sql
  --
  --SET your date minimum of interest
CREATE TEMP FUNCTION
  minimum() AS (TIMESTAMP("2012-01-01"));
  --
  --SET your date maximum of interest
CREATE TEMP FUNCTION
  maximum() AS (TIMESTAMP("2020-12-31"));
  --
  --
  -----------------------------------------
  -- Get table of ports and PSMA status/date
  -----------------------------------------
WITH
  poc_info AS(
  SELECT
    iso3,
    date_rat_acc_app AS date_poc,
    TRUE ratified
  FROM
    `world-fishing-827.prj_forced_labor.foc_poc_database_v20210304`     -- note: We need to understand this better and update POC
  WHERE
    NOT date_rat_acc_app IS NULL -- seems that ratifying is all that matters
    ),
  ------------------------------------------
  -- Get vessel info
  ------------------------------------------
  vessel_info AS(
  SELECT
    year,
    ssvid,
    best.best_flag AS flag
  FROM
    `world-fishing-827.gfw_research.vi_ssvid_byyear_v20210301` ),
  ------------------------------------------
  -- assign PSMA info anchorage (use for from
    -- anchorages)
  ------------------------------------------
  from_anchorage_info AS (
  SELECT
    s2id AS from_anchorage_id,
    iso3 AS from_country,
    ratified AS from_ratified,
    date_poc AS from_date_poc,
    lat AS from_anchorage_lat,
    lon AS from_anchorage_lon
  FROM
    `world-fishing-827.anchorages.named_anchorages_v20201104`
  LEFT JOIN
    poc_info
  USING
    (iso3)),
  ------------------------------------------
  -- Duplicate of above for the to anchorages
  ------------------------------------------
  to_anchorage_info AS (
  SELECT
    s2id AS to_anchorage_id,
    iso3 AS to_country,
    ratified AS to_ratified,
    date_poc AS to_date_poc,
    lat AS to_anchorage_lat,
    lon AS to_anchorage_lon
  FROM
    `world-fishing-827.anchorages.named_anchorages_v20201104`
  LEFT JOIN
    poc_info
  USING
    (iso3)),
  ------------------------------------------
  -- Get voyages without noise (join on vessel
    -- info as a filter)
  ------------------------------------------
  master AS (
  SELECT
    ssvid,
    year,
    flag,
    trip_start_date,
    trip_start
    trip_end,
    from_anchorage_id,
    to_anchorage_id,
    trip_id,
    TIMESTAMP_DIFF(trip_end,trip_start,HOUR) AS trip_duration
  FROM
    vessel_info
  JOIN (
    SELECT
      ssvid,
      EXTRACT(year
      FROM
        trip_start) AS year,
      EXTRACT(date
      FROM
        trip_start) AS trip_start_date,
      trip_start,
      trip_end,
      trip_start_anchorage_id AS from_anchorage_id,
      trip_end_anchorage_id AS to_anchorage_id,
      trip_id
    FROM
      `world-fishing-827.gfw_research.voyages_no_overlapping_short_seg_v20210226`
    WHERE
      trip_start >= minimum()
      AND trip_end <= maximum())
  USING
    (ssvid,
      year)),
  ------------------------------------------
  -- Assign voyages to ports and label as
  -- ports of convenience
  ------------------------------------------
  joined AS (
  SELECT
    *,
  IF
    (NOT flag = from_country,
      1,
      0) from_country_foreign,
  IF
    (NOT flag = to_country,
      1,
      0) to_country_foreign,
    (CASE
        WHEN year < 2016 THEN 0
        WHEN from_ratified IS NULL THEN 1
        WHEN from_ratified AND trip_start_date >= from_date_poc THEN 0
      ELSE
      1
    END
      ) AS from_country_poc,
    (CASE
        WHEN year < 2016 THEN 0
        WHEN to_ratified IS NULL THEN 1
        WHEN to_ratified AND trip_start_date >= to_date_poc THEN 0
      ELSE
      1
    END
      ) AS to_country_poc
  FROM
    master
  LEFT JOIN
    from_anchorage_info
  USING
    (from_anchorage_id)
  LEFT JOIN
    to_anchorage_info
  USING
    (to_anchorage_id))
SELECT
  ssvid,
  year,
  flag,
  trip_id,
  trip_duration,
  from_country,
  from_country_foreign,
  from_country_poc,
  from_anchorage_id,
  from_anchorage_lat,
  from_anchorage_lon,
  to_country,
  to_country_foreign,
  to_country_poc,
  to_anchorage_id,
  to_anchorage_lat,
  to_anchorage_lon
FROM
  joined
