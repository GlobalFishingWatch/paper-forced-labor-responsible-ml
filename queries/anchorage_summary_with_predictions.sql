 -----------------------------------------
  -- G. McDonald, August 19, 2022
  -- Summarize the number of anchorages visit by yearand class prediction

#standardSQL
WITH
  from_anchorages AS(
  SELECT
    year,
    CAST(class_mode AS STRING) class_mode,
    from_anchorage_id anchorage_id
  FROM
    `world-fishing-827.prj_forced_labor.voyage_summary_with_predictions_by_port`),
  to_anchorages AS(
  SELECT
    year,
    CAST(class_mode AS STRING) class_mode,
    to_anchorage_id anchorage_id
  FROM
    `world-fishing-827.prj_forced_labor.voyage_summary_with_predictions_by_port`),
  all_anchorages AS(
  SELECT
    *
  FROM
    from_anchorages
  UNION ALL
  SELECT
    *
  FROM
    to_anchorages )
SELECT
  year,
  class_mode,
  COUNT(DISTINCT anchorage_id) AS unique_anchorages
FROM
  all_anchorages
GROUP BY
  year,
  class_mode
UNION ALL
SELECT
  year,
  'All visits' class_mode,
  COUNT(DISTINCT anchorage_id) AS unique_anchorages
FROM
  all_anchorages
GROUP BY
  year

