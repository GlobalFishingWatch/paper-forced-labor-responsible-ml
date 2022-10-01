-- !preview conn=DBI::dbConnect(bigrquery::bigquery(),project = "world-fishing-827", use_legacy_sql = FALSE)
#standardSQL
-----------------------------------------
  -- Getting a list of iuu vessels

-- R. Joo v. 2021-03-15
-----------------------------------------


-----------------------------------------
-- Jaeyoon's query that pulls a list of the matched vessels recorded in the TMT IUU list.
-- JP: I’ve added a list of all registries to which the vessels are recorded,
-- the latest date of record of the vessel in the TMT list, and detailed notes about their being IUU written in the TMT website.
-- You can add 44 more vessels if you include loose_match on top of matched ones (a bunch of Chinese and Indian vessels).
-- If you need the raw list of the TMT IUU list, let me know.
-----------------------------------------

WITH iuu_gfw AS (

SELECT
  identity.ssvid, identity.n_shipname, identity.n_callsign, identity.imo, identity.flag,
  ARRAY_TO_STRING (feature.geartype, "|") AS geartype,
  (SELECT MIN (first_timestamp) FROM UNNEST (activity)) AS first_timestamp,
  (SELECT MAX (last_timestamp) FROM UNNEST (activity)) AS last_timestamp,
  (SELECT SUM (messages) FROM UNNEST (activity)) AS messages,
  ARRAY_TO_STRING (
    ARRAY (
      SELECT DISTINCT udfs.extract_regcode (list_uvi)
      FROM UNNEST (registry)
      WHERE list_uvi IS NOT NULL ), ", ") AS list_uvi,
  (SELECT MAX (scraped) FROM UNNEST (registry) WHERE list_uvi LIKE "IUU%") AS latest_record,
  ( SELECT DISTINCT notes
    FROM UNNEST (registry)
    WHERE list_uvi LIKE "IUU%"
      AND notes IS NOT NULL
      AND scraped = (
        SELECT MAX (scraped)
        FROM UNNEST (registry)
        WHERE list_uvi LIKE "IUU%" ) ) AS notes

FROM `vessel_database.all_vessels_v20220801`
WHERE matched #loose_match
  AND EXISTS (
    SELECT *
    FROM UNNEST (registry)
    WHERE list_uvi LIKE "IUU%")

    ),

-----------------------------------------
-- Get list of IUU vessels from Gavin's table
-----------------------------------------
  iuu_ucsb AS(
  SELECT
    CAST(mmsi AS STRING) AS ssvid,
    CAST(imo AS STRING) AS ucsb_imo,
    callsign_norm AS ucsb_callsign_norm,
    shipname_norm AS ucsb_shipname_norm
  FROM
    `prj_forced_labor.iuu_ucsb`)

-----------------------------------------
-- Joining both
-----------------------------------------

  SELECT
    ssvid, --I don't think we need more than ssvid so I shouldn't be adding more than that
    IF (n_shipname IS NULL, ucsb_shipname_norm, n_shipname) AS shipname_norm,
    IF (imo IS NULL, ucsb_imo, imo) AS imo,
    IF (n_callsign IS NULL, ucsb_callsign_norm, n_callsign) AS callsign_norm,
  FROM
    iuu_gfw
  FULL JOIN (
    SELECT
      ssvid,
      ucsb_shipname_norm,
      ucsb_imo,
      ucsb_callsign_norm
    FROM
    iuu_ucsb
      )
      USING(ssvid)
