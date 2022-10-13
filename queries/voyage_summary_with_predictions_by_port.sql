  -----------------------------------------
  -- G. McDonald, August 19, 2022
  -- Summarize the number of voyages by year, from_anchorage_id, to_anchorage_id, and class prediction
  -----------------------------------------
  #standardSQL
WITH
  -----------------------------------------
  -- Summarize number of voyages, by ssvid, year, and to/from countries
  -----------------------------------------
  voyage_summary AS(
  SELECT
    ssvid,
    year,
    flag,
    from_anchorage_id,
    to_anchorage_id,
    COUNT(DISTINCT trip_id) number_voyages
  FROM
    `world-fishing-827.prj_forced_labor.all_voyages_by_ssvid`
  GROUP BY
    ssvid,
    year,
    flag,
    from_anchorage_id,
    to_anchorage_id),
  -----------------------------------------
  -- Get latest forced labor predictions
  -----------------------------------------
  predictions AS(
  SELECT
    ssvid,
    year,
    class_mode
  FROM
    `world-fishing-827.prj_forced_labor.pred_stats_per_vessel_year_dev_2021` ),
  joined AS(
  SELECT
    *
  FROM
    voyage_summary
  JOIN
    predictions
  USING
    (ssvid,
      year))
SELECT
  year,
  from_anchorage_id,
  to_anchorage_id,
  class_mode,
  SUM(number_voyages) number_voyages
FROM
  joined
GROUP BY
  year,
  from_anchorage_id,
  to_anchorage_id,
  class_mode
