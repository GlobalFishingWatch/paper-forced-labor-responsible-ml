  -- !preview conn=DBI::dbConnect(bigrquery::bigquery(),project = "world-fishing-827", use_legacy_sql = FALSE)
  #standardSQL
  -----------------------------------------
  -- Query to get voyages by vessel/year along
  -- with the number of visits to Ports of
  -- Convenience (defined here as ports in
  -- countries that have not ratified PSMA)
  --
  -- R.Joo  v2022-09-14
  -- Adapted from all_port_visits_param.sql by G. McDonald on 2022-10-17, so that
  -- final aggregation step is not performed and we get a row for each voyage
  --
  --SET your date minimum of interest
CREATE TEMP FUNCTION
  minimum() AS (TIMESTAMP("2012-01-01"));
  --
  --SET your date maximum of interest
CREATE TEMP FUNCTION
  maximum() AS (TIMESTAMP("2021-12-31"));
  --
  --
  -----------------------------------------
  -- Get table of ports and PSMA status/date
  -----------------------------------------
WITH
  poc_info AS (
  SELECT
    *,
    (CASE
        WHEN (NOT signature IS NULL AND ratification IS NULL AND acceptance IS NULL AND approval IS NULL AND accession IS NULL) THEN signature
        WHEN (NOT ratification IS NULL
        AND acceptance IS NULL
        AND approval IS NULL
        AND accession IS NULL) THEN ratification
        WHEN (NOT acceptance IS NULL AND ratification IS NULL AND approval IS NULL AND accession IS NULL) THEN acceptance
        WHEN (NOT approval IS NULL
        AND ratification IS NULL
        AND acceptance IS NULL
        AND accession IS NULL) THEN approval
        WHEN (NOT accession IS NULL AND ratification IS NULL AND acceptance IS NULL AND approval IS NULL) THEN accession
        WHEN (NOT approval IS NULL
        AND NOT accession IS NULL) THEN LEAST(approval, accession)
      ELSE
      NULL
    END
      ) AS date_psma
  FROM
    `world-fishing-827.gfw_research.psma_signatories_v20220907` ),
  -- the last condition was for the UK but there could be other cases
  -- sanity check
  --SELECT
  --*
  --FROM poc_info
  --WHERE date_psma IS NULL
  poc_info_summary AS (
  SELECT
    iso3,
    date_psma,
    TRUE ratified
  FROM
    poc_info ),
  ------------------------------------------
  -- Get vessel info
  ------------------------------------------
  -- First get from vessel_info_all_tidy
  vessel_info_tidy AS (
  SELECT
    ssvid,
    year,
    flag
  FROM
    `world-fishing-827.prj_forced_labor.vessel_info_all_tidy`),
  -- Now get from vessel_info_all_messy
  vessel_info_messy AS (
  SELECT
    ssvid,
    year,
    flag
  FROM
    `world-fishing-827.prj_forced_labor.vessel_info_all_messy`),
  -- Now put tidy and messy together
  vessel_info_unioned AS(
  SELECT
    *
  FROM
    vessel_info_tidy
  UNION ALL
  SELECT
    *
  FROM
    vessel_info_messy ),
  -- Now take only distinct ssvid/year/flag combos from across tidy and messy
  vessel_info AS(
  SELECT
    DISTINCT ssvid,
    year,
    flag
  FROM
    vessel_info_unioned ),
  ------------------------------------------
  -- assign PSMA info anchorage (use for from
  -- anchorages)
  ------------------------------------------
  from_anchorage_info AS (
  SELECT
    s2id AS from_anchorage_id,
    iso3 AS from_country,
    ratified AS from_ratified,
    date_psma AS from_date_poc
  FROM
    `world-fishing-827.anchorages.named_anchorages_v20220511`
  LEFT JOIN
    poc_info_summary
  USING
    (iso3) ),
  ------------------------------------------
  -- Duplicate of above for the to anchorages
  ------------------------------------------
  to_anchorage_info AS (
  SELECT
    s2id AS to_anchorage_id,
    iso3 AS to_country,
    ratified AS to_ratified,
    date_psma AS to_date_poc
  FROM
    `world-fishing-827.anchorages.named_anchorages_v20220511`
  LEFT JOIN
    poc_info_summary
  USING
    (iso3)),
  ------------------------------------------
  -- Get voyages without noise (join on vessel
  -- info as a filter)
  -- We could change the level of confidence
  ------------------------------------------
  master AS (
  SELECT
    ssvid,
    year,
    trip_start,
    trip_start_date,
    trip_end,
    from_anchorage_id,
    to_anchorage_id,
    trip_id
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
      `world-fishing-827.pipe_production_v20201001.proto_voyages_c4`
    WHERE
      trip_start >= minimum()
      AND trip_end <= maximum())
  USING
    (ssvid,
      year) ),
  ------------------------------------------
  -- Assign voyages to ports and label as
  -- ports of convenience
  ------------------------------------------
  joined AS (
  SELECT
    *,
    TIMESTAMP_DIFF(trip_end,trip_start,HOUR) AS trip_duration,
  IF
    (NOT flag = from_country
      OR NOT flag = to_country, 1, 0) foreign_port,
    (CASE
        WHEN year < 2016 THEN 0
        WHEN to_ratified IS NULL THEN 1
        WHEN from_ratified IS NULL THEN 1
        WHEN to_ratified
      AND trip_start_date >= to_date_poc THEN 0
        WHEN from_ratified AND trip_start_date >= from_date_poc THEN 0
      ELSE
      1
    END
      ) AS poc
  FROM
    master
  LEFT JOIN
    from_anchorage_info
  USING
    (from_anchorage_id)
  LEFT JOIN
    to_anchorage_info
  USING
    (to_anchorage_id)
  LEFT JOIN
    vessel_info
  USING
    (ssvid,
      year))
SELECT
  *
FROM
  joined
