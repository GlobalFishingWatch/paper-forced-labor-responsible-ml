 -----------------------------------------
  -- G. McDonald, August 19, 2022
  -- Summarize the number of anchorages visit by anchorage, location, yearand class prediction

#standardSQL
WITH
  port_predictions AS(
  SELECT
    *
  FROM
    `world-fishing-827.prj_forced_labor.voyage_summary_with_predictions_by_port`),
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
    port_predictions
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
