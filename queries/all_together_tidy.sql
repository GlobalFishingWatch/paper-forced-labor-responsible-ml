-- !preview conn=DBI::dbConnect(bigrquery::bigquery(),project = "world-fishing-827", use_legacy_sql = FALSE)
#standardSQL
-----------------------------------------
  -- Query to put everything together
  --
  -- R.Joo  v2021-03-16

-----------------------------------------

  WITH

  -----------------------------------------
  -- Load vessel info (per vessel per year)
  -----------------------------------------

  main_vessel_info AS(
  SELECT
    * -- EXCEPT(
      -- shipname, -- I still want to have the name to check later
      -- imo,
      -- ais_imo) --,
      --on_fishing_list_best) --shouldn't be necessary anymore
  FROM
    `prj_forced_labor.vessel_info_all_tidy`
  ),
  -- WHERE
    --on_fishing_list_best),


-----------------------------------------------
-- Load offenders info (per forced labor event)
-----------------------------------------------


  offender_vessel_info AS(
  SELECT
    ssvid,
    year,
    year_focus,
    num_years,
    past_ais_year,
    event_ais_year,
    fl_event_id,
    source_id,
    known_offender,
    known_non_offender,
    possible_offender,
    fl_abuse_of_vulnerability,
    fl_deception,
    fl_restriction_of_movement,
    fl_isolation,
    fl_physical_and_sexual_violence,
    fl_intimidation_and_threats,
    fl_retention_of_identity_documents,
    fl_withholding_of_wages,
    fl_debt_bondage,
    fl_abusive_working_and_living_conditions,
    fl_excessive_overtime,
    fl_trafficking
  FROM
    `prj_forced_labor.vessel_info_forced_labor_2`
    ),

-----------------------------------------------
-- Merging all events data (fishing,
-- encounters, loitering, port visits)
-- with vessel information for all fishing
-- vessels (offenders and non-offenders)
-----------------------------------------------


  joined AS(
  SELECT
    *
  FROM
    main_vessel_info
  LEFT JOIN
    offender_vessel_info
    USING(
      ssvid,
      year)
  LEFT JOIN (
    SELECT
      *
    FROM
      `prj_forced_labor.all_fishing_tidy`)
      USING(
        ssvid,
        year)
  LEFT JOIN (
    SELECT
      *
    FROM
      `prj_forced_labor.all_encounters`)
      USING(
        ssvid,
        year)
  LEFT JOIN (
    SELECT
      *
    FROM
      `prj_forced_labor.all_gaps`)
      USING(
        ssvid,
        year)
  LEFT JOIN (
    SELECT
      *
    FROM
      `prj_forced_labor.all_port_visits_tidy`)
      USING(
        ssvid,
        year)
    LEFT JOIN (
    SELECT
      *
    FROM
      `world-fishing-827.prj_forced_labor.all_loitering`)
      USING(
        ssvid,
        year)
),

-----------------------------------------------
-- Excluding vessels with problematic AIS
-- from AIS East Asia
-----------------------------------------------

non_problematic as (
  SELECT
    *
  FROM
    joined
  WHERE
      NOT ssvid IN(
        SELECT
          ssvid
        FROM
          `prj_forced_labor.problematic_AIS_EastAsia`)
      AND distance_traveled_km >0 AND hours > 0 AND position_messages > 100
      -- seems that some vessels do not have positions but appear in vessel_info
      -- we want to make sure that they were at sea for at least 100 positions to
      -- trust their AIS stats and use them in the model
  ORDER BY
    fl_event_id DESC,
    ssvid,
    year
)

---------------------------------------------------
-- For vessels without voyage,
-- say it's one voyage throughout the year
-- and the duration of the voyage is equal to hours
----------------------------------------------------

SELECT *
  EXCEPT(average_voyage_duration_hours, number_voyages),
  IF (number_voyages is NULL, hours, average_voyage_duration_hours) as average_voyage_duration_hours,
  IF (number_voyages is NULL, 1, number_voyages) as number_voyages,
FROM
  non_problematic
 GROUP BY 1,2,3,4,5,6,7,8,9,10,
          11,12,13,14,15,16,17,
          18,19,20,21,22,23,24,
          25,26,27,28,29,30,31,
          32,33,34,35,36,37,38,
          39,40,41,42,43,44,45,
          46,47,48,49,50,51,52,
          53,54,55,56,57,58,59,
          60,61,62,63,64

