  -----------------------------------------
  -- G. McDonald, November 17, 2022
  -- Summarize the number of voyages by year, from_anchorage_label, to_anchorage_label, and class prediction
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
    from_anchorage_id,
    to_anchorage_id,
    COUNT(DISTINCT trip_id) number_voyages
  FROM
    `world-fishing-827.prj_forced_labor.all_voyages_by_ssvid`
  GROUP BY
    ssvid,
    year,
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
  from_port_locations AS(
  SELECT
    s2id from_anchorage_id,
    label from_anchorage_label,
    sublabel from_anchorage_sublabel,
    lat from_lat,
    lon from_lon,
    iso3 from_country
  FROM
    `world-fishing-827.anchorages.named_anchorages_v20220511` ),
  to_port_locations AS(
  SELECT
    s2id to_anchorage_id,
    label to_anchorage_label,
    sublabel to_anchorage_sublabel,
    lat to_lat,
    lon to_lon,
    iso3 to_country
  FROM
    `world-fishing-827.anchorages.named_anchorages_v20220511` ),
  joined AS(
  SELECT
    *
  FROM
    voyage_summary
  JOIN
    predictions
  USING
    (ssvid,
      year)
  LEFT JOIN
    from_port_locations
  USING
    (from_anchorage_id)
  LEFT JOIN
    to_port_locations
  USING
    (to_anchorage_id))
SELECT
  class_mode,
  year,
  from_anchorage_label,
  from_anchorage_sublabel,
  to_anchorage_label,
  to_anchorage_sublabel,
  from_lat,
  from_lon,
  from_country,
  to_lat,
  to_lon,
  to_country,
  SUM(number_voyages) number_voyages
FROM
  joined
GROUP BY
  class_mode,
  year,
  from_anchorage_label,
  from_anchorage_sublabel,
  to_anchorage_label,
  to_anchorage_sublabel,
  from_lat,
  from_lon,
  from_country,
  to_lat,
  to_lon,
  to_country
