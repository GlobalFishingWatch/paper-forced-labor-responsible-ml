# Read me of the queries:

All of these queries can be run from the R/queries_run.r script. 

The tables are in `world-fishing-827:prj_forced_labor`.

They are run in the following order:



## all_loitering.sql

It computes loitering event stats per vessel-year. 
Loitering events happen when vessels travel 
at speeds of less than 2 knots for a period of time. We use:

* `world-fishing-827.pipe_production_v20201001.loitering` to get the
loitering events 
* `world-fishing-827.pipe_production_v20201001.research_segs` to get the good segments

In this query, we calculate the following variables used
in the model: number_loitering_events, 
average_loitering_duration_hours



## all_gaps.sql

It computes gap stats per vessel-year, using:

* `world-fishing-827.pipe_production_v20201001.proto_published_events_ais_gaps`; 
for this query we add a few conditions to narrow 
down possible intentional gaps using the wiki.

In this query, we calculate variables used in the model like gaps_12_hours.



## problematic_AIS_EastAsia.sql

It identifies vessels that primarily fish in certain East Asian EEZ's with likely
bad AIS reception, using:

* `world-fishing-827.gfw_research.vi_ssvid_v20220101` with info on time spent in
eezs
* `world-fishing-827.gfw_research.eez_info` to get the countries corresponding to
the eezs



## all_iuu.sql

It generates a table with IUU vessels from:

* `prj_forced_labor.iuu_ucsb`: Gavin's set
* `vessel_database.all_vessels_v20220801`: Jaeyoon's query 



## vessel_info_all_tidy.sql

It generates a general vessel info table per year, using:

* `world-fishing-827.gfw_research.flags_of_convenience_v20211013` for foc
* `prj_forced_labor.all_iuu` for iuu, generated with `all_iuu.sql`
* `gfw_research.fishing_vessels_ssvid_v20220801` to get only fishing vessels
* `world-fishing-827.gfw_research.vi_ssvid_byyear_v20220801` to get ais type 
for each vessel by year and other general info
 
From this query, we obtain a table with variables used in the model, 
such as: year, gear, flag, iuu, ais_type, crew_size, engine_power_kw, foc,
length_m, tonnage_gt



## vessel_info_all_messy.sql

It generates a general vessel info table per year, using:

* `world-fishing-827.gfw_research.flags_of_convenience_v20211013` for foc
* `prj_forced_labor.all_iuu` for iuu, generated with `all_iuu.sql`
* `world-fishing-827.gfw_research.vi_ssvid_byyear_v20220801` to get ais type 
for each vessel by year and other general info
We get more vessels, and some messier than with the tidy query.



## all_vessel_positions_tidy.sql

It extracts vessel positions and some metadata using:

* `world-fishing-827.gfw_research.fishing_vessels_ssvid_v20220801` to get best
vessel class from neural net classification
* `world-fishing-827.pipe_production_v20201001.research_messages` for positions of 
fishing vessels, and additional info such as `seg_id`, time and distance to 
previous AIS record, nnet score for fishing, night loitering, distance from 
shore, distance from port, speed, eez 
* `world-fishing-827.pipe_production_v20201001.research_segs` to get info on good
segment ids

From this query, we obtain information to calculate variables used in the model, 
such as: positions, hours, fishing_hours, 
average_daily_fishing_hours, distance_traveled_km, max_distance_from_port_km,
max_distance_from_shore_km. These variables are actually calculated in 
`all_fishing_param.sql`



## all_vessel_positions_messy.sql

It extracts vessel positions and some metadata using:

* `world-fishing-827.gfw_research.vi_ssvid_byyear_v20220801` to get vessel class
* `world-fishing-827.pipe_production_v20201001.research_messages` for positions of 
fishing vessels, and additional info such as `seg_id`, time and distance to 
previous AIS record, nnet score for fishing, night loitering, distance from 
shore, distance from port, speed, eez 
* `world-fishing-827.pipe_production_v20201001.research_segs` to get info on good
segment ids

From this query, we obtain information to calculate variables used in the model,
such as: positions, hours, fishing_hours, 
average_daily_fishing_hours, distance_traveled_km, max_distance_from_port_km,
max_distance_from_shore_km. These variables are actually calculated in 
`all_fishing_param.sql`



## all_fishing_param.sql

It summarizes all the fishing related info per vessel-year, using:

* `prj_forced_labor.all_vessel_positions` (`_tidy` or `_messy`) to get info at 
the scale of positions, generated with `all_vessel_positions.sql`
* `world-fishing-827.gfw_research.eez_info` to get `eez_iso3`
* `prj_forced_labor.vessel_info_all` (`_tidy` or `_messy`) to get ssvid, year and 
flag, needed with the preceding tables to identify fishing at high seas and in
foreign eezs

Besides the variables mentioned in the previous section, here we obtain the 
following variables used in the model: 
fishing_hours_foreign_eez, fishing_hours_high_seas. 

* It's written as a parameterized query.



## all_port_visits_param.sql

It computes voyage and port visit stats per vessel-year, using:

* `world-fishing-827.gfw_research.psma_signatories_v20220907` which has poc info 
by country. 
* `world-fishing-827.anchorages.named_anchorages_v20220511` to get anchorage
events
* `world-fishing-827.pipe_production_v20201001.proto_voyages_c4`
which helps only counting anchorage from "real" voyages and not short segments
* `world-fishing-827.prj_forced_labor.vessel_info_all` (`_tidy` or `_messy`) 
to get the vessels, year and flag, generated with the corresponding
 `vessel_info_all.sql`

In this query, we calculate the following variables used
in the model: number_voyages, average_voyage_duration_hours,
number_poc_port_visits, number_foreign_port_visits

* It's written as a parameterized query.



## vessel_info_forced_labor_1.sql and vessel_info_forced_labor_2.sql

It joins the forced labor data with the general vessel info table, 
only keeping one row per forced labor event. 
I had to cut in two because the joins were too long.
They have to be run one after the other.
They use:

* `prj_forced_labor.vessel_info_all_messy`: the vessel info table generated with 
`vessel_info_all_messy.sql`
* `prj_forced_labor.forced_labor`: cleaned forced labor dataset generated with 
`/R/preprocess_to_forced_labor_table.r` 



## all_encounters.sql

It computes encounter stats per vessel-year, using:

* `world-fishing-827.pipe_production_v20201001.published_events_encounters` with
encounter events, but ugly vessel id
* `world-fishing-827.pipe_production_v20201001.vessel_info` to get the 
corresponding ssvid and not work with the ugly vessel id anymore
* `prj_forced_labor.vessel_info_forced_labor_2` with the forced labor events, generated
with `vessel_info_forced_labor_2.sql`
* `prj_forced_labor.all_iuu` with iuu vessels, generated with `all_iuu.sql`

In this query, we calculate the following variables used
in the model: number_encounters, average_encounter_duration_hours,
number_forced_labor_encounters, number_iuu_encounters.



# all_fl_ais.sql

It joins every messy or standard table with forced labor events. 
Almost everything above has to be run first. 
It uses:

* `prj_forced_labor.vessel_info_all_messy`
* `prj_forced_labor.vessel_info_forced_labor_2`
* `prj_forced_labor.all_fishing_messy`
* `prj_forced_labor.all_encounters`
* `prj_forced_labor.all_gaps`
* `prj_forced_labor.all_port_visits_messy`
* `prj_forced_labor.all_loitering`
* `prj_forced_labor.problematic_AIS_EastAsia`



## all_together_tidy.sql

Joins every tidy or standard table with
forced labor events. 
Almost everything above has to be run first.

* `prj_forced_labor.vessel_info_all_tidy`
* `prj_forced_labor.vessel_info_forced_labor_2`
* `prj_forced_labor.all_fishing_tidy`
* `prj_forced_labor.all_encounters`
* `prj_forced_labor.all_gaps`
* `prj_forced_labor.all_port_visits_tidy`
* `prj_forced_labor.all_loitering`
* `prj_forced_labor.problematic_AIS_EastAsia`











