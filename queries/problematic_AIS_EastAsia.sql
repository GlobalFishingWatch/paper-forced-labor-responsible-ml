-- !preview conn=DBI::dbConnect(bigrquery::bigquery(),project = "world-fishing-827", use_legacy_sql = FALSE)
#standardSQL
-----------------------------------------
  -- Query to identify vessels only
  -- navigating in East Asia with
  -- potential problematic AIS data

  -- R.Joo  v2021-03-12
-- WITH
  -- Filter out vessels that primarily navigate in certain East Asian EEZ's with bad AIS reception

  SELECT
    ssvid,
    SUM(IF(eez_id_table.eez_id IS NOT NULL, hours,0))/SUM(hours) AS fraction_in_e_asia
  FROM (
    SELECT
      ssvid,
      eez.value AS eez,
      eez.fishing_hours AS fishing_hours,
      eez.hours AS hours
    FROM
      `world-fishing-827.gfw_research.vi_ssvid_v20220101`
    CROSS JOIN
      UNNEST(activity.eez) AS eez
    WHERE
      activity.positions > 100) AS vessel_eez_table
  LEFT JOIN (
    SELECT
      eez_id
    FROM
      `world-fishing-827.gfw_research.eez_info`
    WHERE
      territory1_iso3 IN ('TWN',
        'JPN',
        'CHN',
        'KOR',
        'PRK')) AS eez_id_table
  ON
    vessel_eez_table.eez = CAST(eez_id_table.eez_id AS string)
  GROUP BY
    ssvid
  HAVING
    fraction_in_e_asia > .95
