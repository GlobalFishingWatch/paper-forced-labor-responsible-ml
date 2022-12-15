# Running sql queries

# # packages
# bigrquery
# glue
# readr

# BQ project

gfw_project <- "world-fishing-827"
gfw_dataset <- "prj_forced_labor"
query_path <- "./queries/"


###############################################################################
# all loitering
###############################################################################
# It computes loitering event stats per vessel-year.
# Loitering events happen when vessels travel at speeds of < 2 knots
# for a period of time. We use:
# world-fishing-827.pipe_production_v20201001.loitering
# world-fishing-827.pipe_production_v20201001.research_segs

vessel_stats_loit_sql <- readr::read_file(paste0(query_path,"all_loitering.sql"))
# Run new query. Delete old table, upload new one
bigrquery::bq_project_query(x = gfw_project, query = vessel_stats_loit_sql,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "all_loitering",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")
# Billed: 37.75 GB
# 11s



###############################################################################
# all gaps
###############################################################################
# It computes gap stats per vessel-year, using:
# world-fishing-827.pipe_production_v20201001.proto_published_events_ais_gaps

vessel_stats_gaps_sql <- readr::read_file(paste0(query_path,"all_gaps.sql"))
# Run new query. Delete old table, upload new one
bigrquery::bq_project_query(x = gfw_project, query = vessel_stats_gaps_sql,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "all_gaps",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")
# Billed:  559.94 MB
# 5s



###############################################################################
# problematic AIS East Asia
###############################################################################
# It identifies vessels that primarily fish in certain East Asian EEZ's
# with likely bad AIS reception, using:
# world-fishing-827.gfw_research.vi_ssvid_v20220101
# world-fishing-827.gfw_research.eez_info

problematic_AIS_EastAsia <- readr::read_file(paste0(query_path,"problematic_AIS_EastAsia.sql"))

bigrquery::bq_project_query(x = gfw_project, query = problematic_AIS_EastAsia,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "problematic_AIS_EastAsia",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")

# Billed: 121.63 MB
# 5s



###############################################################################
# iuu vessels
###############################################################################
# It generates a table with IUU vessels from:
# prj_forced_labor.iuu_ucsb
# vessel_database.all_vessels_v20220801

# ucsb_project <- ucsb-gfw
# bq_table(project = "ucsb-gfw",table = "iuu_list_final",dataset = "human_rights") %>%
#   bq_table_copy(dest = "world-fishing-827.prj_forced_labor.iuu_ucsb") #

iuu_sql <- readr::read_file(paste0(query_path,"all_iuu.sql"))

# Run new query. Delete old table, upload new one
bigrquery::bq_project_query(x = gfw_project, query = iuu_sql,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "all_iuu",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")

# Billed: 3.7 GB
# 3s



###############################################################################
# vessel info from tidy vessels
# needs all_iuu.sql to be run before
###############################################################################
# It generates a general vessel info table per year, using:
# world-fishing-827.gfw_research.flags_of_convenience_v20211013
# prj_forced_labor.all_iuu for iuu, generated with all_iuu.sql
# gfw_research.fishing_vessels_ssvid_v20220801 to get only fishing vessels
# world-fishing-827.gfw_research.vi_ssvid_byyear_v20220801

vessel_info_tidy_sql <- readr::read_file(paste0(query_path,"vessel_info_all_tidy.sql"))

# Run new query. Delete old table, upload new one
bigrquery::bq_project_query(x = gfw_project, query = vessel_info_tidy_sql,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "vessel_info_all_tidy",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")
# BQ's legacy SQL format id not an updated format
# "WRITE_TRUNCATE": If the table already exists, BigQuery overwrites the table data.
# Billed: 627.05 MB
# 11s.



###############################################################################
# vessel info from messy vessels (to get more offenders)
# needs all_iuu.sql to be run before
###############################################################################
# It generates a general vessel info table per year, using:
# world-fishing-827.gfw_research.flags_of_convenience_v20211013
# prj_forced_labor.all_iuu for iuu, generated with all_iuu.sql
# world-fishing-827.gfw_research.vi_ssvid_byyear_v20220801

vessel_info_messy_sql <- readr::read_file(paste0(query_path,"vessel_info_all_messy.sql"))

# Run new query. Delete old table, upload new one
bigrquery::bq_project_query(x = gfw_project, query = vessel_info_messy_sql,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "vessel_info_all_messy",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")
# Billed: 681.57 MB
# 14s.



###############################################################################
# all vessel positions from tidy vessels
###############################################################################
# It extracts vessel positions and some metadata using:
# world-fishing-827.gfw_research.fishing_vessels_ssvid_v20220801
# world-fishing-827.pipe_production_v20201001.research_messages
# world-fishing-827.pipe_production_v20201001.research_segs

vessel_info_positions_tidy_sql <- readr::read_file(paste0(query_path,
                                                   "all_vessel_positions_tidy.sql"))
# Run new query. Delete old table, upload new one
bigrquery::bq_project_query(x = gfw_project, query = vessel_info_positions_tidy_sql,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "all_vessel_positions_tidy",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")
# With "2020-12-31" it should be 915.06 GB # 2m
# With "2021-12-31" it should be 1.2 TB # 6m

###############################################################################
# all vessel positions from messy vessels
###############################################################################
# It extracts vessel positions and some metadata using:
# world-fishing-827.gfw_research.vi_ssvid_byyear_v20220801
# world-fishing-827.pipe_production_v20201001.research_messages
# world-fishing-827.pipe_production_v20201001.research_segs

vessel_info_positions_messy_sql <- readr::read_file(paste0(query_path,
                                                   "all_vessel_positions_messy.sql"))
# Run new query. Delete old table, upload new one
bigrquery::bq_project_query(x = gfw_project, query = vessel_info_positions_messy_sql,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "all_vessel_positions_messy",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")
# With "2020-12-31" it should be 915.06 GB # 2m
# With "2021-12-31" it should be 1.2 TB # 8m


###############################################################################
# all fishing
# needs both vessel_info_all and
# all_vessel_positions queries
# to be run before
###############################################################################
# It summarizes all the fishing related info per vessel-year


# For tidy data, using:
# prj_forced_labor.vessel_info_all_tidy
# world-fishing-827.gfw_research.eez_info
# prj_forced_labor.all_vessel_positions_tidy

vessel_info_all_table <- "prj_forced_labor.vessel_info_all_tidy"
all_vessel_positions_table <- "prj_forced_labor.all_vessel_positions_tidy"

vessel_stats_fishing_sql <- glue::glue(
  read_file(paste0(query_path,"all_fishing_param.sql"))
)


# Run new query. Delete old table, upload new one
bigrquery::bq_project_query(x = gfw_project, query = vessel_stats_fishing_sql,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "all_fishing_tidy",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")
# Billed: 260.86 GB
# 1m


# For messy data, using:
# prj_forced_labor.all_vessel_positions_messy
# world-fishing-827.gfw_research.eez_info
# prj_forced_labor.vessel_info_all_messy

vessel_info_all_table <- "prj_forced_labor.vessel_info_all_messy"
all_vessel_positions_table <- "prj_forced_labor.all_vessel_positions_messy"

vessel_stats_fishing_sql <- glue::glue(
  read_file(paste0(query_path,"all_fishing_param.sql"))
)

# Run new query. Delete old table, upload new one
bigrquery::bq_project_query(x = gfw_project, query = vessel_stats_fishing_sql,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "all_fishing_messy",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")
# Billed: 296.85 GB-- for the complete one
# 1m



###############################################################################
# all ports visits
# needs vessel_info_all to be run first
###############################################################################
# It computes voyage and port visit stats per vessel-year, using:
# world-fishing-827.gfw_research.psma_signatories_v20220907
# world-fishing-827.anchorages.named_anchorages_v20220511
# world-fishing-827.pipe_production_v20201001.proto_voyages_c4
# world-fishing-827.prj_forced_labor.vessel_info_all_tidy or _messy depending
# on which port information we're looking for

# For tidy data
vessel_info_all_table <- "prj_forced_labor.vessel_info_all_tidy"

vessel_stats_ports_sql <- glue::glue(
  read_file(paste0(query_path,"all_port_visits_param.sql"))
)

# Run new query. Delete old table, upload new one
bigrquery::bq_project_query(x = gfw_project, query = vessel_stats_ports_sql,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "all_port_visits_tidy",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")
# Billed: 5.57 GB
# 10s

# For messy data
vessel_info_all_table <- "prj_forced_labor.vessel_info_all_messy"

vessel_stats_ports_sql <- glue::glue(
  read_file(paste0(query_path,"all_port_visits_param.sql"))
)


# Run new query. Delete old table, upload new one
bigrquery::bq_project_query(x = gfw_project, query = vessel_stats_ports_sql,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "all_port_visits_messy",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")
# Billed: 5.58 GB
# 11s



###############################################################################
# vessel info forced labor
# needs vessel_info_all query to be run first
###############################################################################
# It joins the forced labor data with the general vessel info table,
# only keeping one row per forced labor event. It uses:
# prj_forced_labor.vessel_info_all_messy
# prj_forced_labor.forced_labor
# It has two parts because of too many joins

vessel_info_forced_labor_1_sql <- readr::read_file(paste0(query_path,"vessel_info_forced_labor_1.sql"))
# Run new query. Delete old table, upload new one
bigrquery::bq_project_query(x = gfw_project, query = vessel_info_forced_labor_1_sql,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "vessel_info_forced_labor_1",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")
# Billed: 95.42 MB
# 6s

vessel_info_forced_labor_2_sql <- readr::read_file(paste0(query_path,"vessel_info_forced_labor_2.sql"))
# Run new query. Delete old table, upload new one
bigrquery::bq_project_query(x = gfw_project, query = vessel_info_forced_labor_2_sql,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "vessel_info_forced_labor_2",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")
# Billed: 96.47 MB
# 10s


###############################################################################
# all encounters
# needs vessel info offenders to be run before
###############################################################################
# It computes encounter stats per vessel-year, using:
# world-fishing-827.pipe_production_v20201001.published_events_encounters
# world-fishing-827.pipe_production_v20201001.vessel_info
# prj_forced_labor.vessel_info_forced_labor_2
# prj_forced_labor.all_iuu

all_encounters_sql <- readr::read_file(paste0(query_path,"all_encounters.sql"))
# Run new query. Delete old table, upload new one
bigrquery::bq_project_query(x = gfw_project, query = all_encounters_sql,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "all_encounters",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")
# Billed: 1.39 GB
# 7s


###############################################################################
# all fl ais
# needs almost everything above to be run first
###############################################################################
# uses
# prj_forced_labor.vessel_info_all_messy
# prj_forced_labor.vessel_info_forced_labor_2
# prj_forced_labor.all_fishing_messy
# prj_forced_labor.all_encounters
# prj_forced_labor.all_gaps
# prj_forced_labor.all_port_visits_messy
# prj_forced_labor.all_loitering
# prj_forced_labor.problematic_AIS_EastAsia
# and gets all the AIS info for known offenders, known non offenders and
# possible offenders (regardless of year)

all_fl_ais_sql <- readr::read_file(paste0(query_path,"all_fl_ais.sql"))

bigrquery::bq_project_query(x = gfw_project, query = all_fl_ais_sql,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "all_fl_ais",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")
# Billed: 306.18 MB
# 8s



###############################################################################
# all together tidy
# needs almost everything above to be run first
###############################################################################
# uses
# prj_forced_labor.vessel_info_all_tidy
# prj_forced_labor.vessel_info_forced_labor_2
# prj_forced_labor.all_fishing_tidy
# prj_forced_labor.all_encounters
# prj_forced_labor.all_gaps
# prj_forced_labor.all_port_visits_tidy
# prj_forced_labor.all_loitering
# prj_forced_labor.problematic_AIS_EastAsia
# and gets all the AIS info for all the tidy vessels

all_together_tidy <- readr::read_file(paste0(query_path,"all_together_tidy.sql"))

bigrquery::bq_project_query(x = gfw_project, query = all_together_tidy,
                 destination_table = bigrquery::bq_table(project = gfw_project,
                                              table = "all_together_tidy",
                                              dataset = gfw_dataset),
                 use_legacy_sql = FALSE, allowLargeResults = TRUE,
                 write_disposition = "WRITE_TRUNCATE")

# Billed: 226.49 MB
# 16 s

