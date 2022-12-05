###################### data pre-processing ##########################

# Establish connection to BigQuery project
con <- DBI::dbConnect(drv = bigrquery::bigquery(),
                      project = "world-fishing-827", use_legacy_sql = FALSE)
# Deal with BQ data download error
# See for reference: https://github.com/r-dbi/bigrquery/issues/395
options(scipen = 20)
gfw_project <- "world-fishing-827"
### Loading offenders vessel-year dataset

ais_fl <- glue::glue(
  "SELECT
      *
    FROM
      `prj_forced_labor.all_fl_ais`
"
)

fl_df <- fishwatchr::gfw_query(query = ais_fl, run_query = TRUE, con = con)
##### PROBLEM HERE SINCE THIS IS A PRIVATE PACKAGE ###########

### Loading all together vessel-year dataset (tidy version)

ais_tidy <- glue::glue(
  "SELECT
      *
    FROM
      `prj_forced_labor.all_together_tidy`
"
)

alltogether_tidy <- fishwatchr::gfw_query(query = ais_tidy, run_query = TRUE,
                                          con = con)
##### PROBLEM HERE SINCE THIS IS A PRIVATE PACKAGE ###########


gears_interest <- c("drifting_longlines", "trawlers", "squid_jigger",
                    "purse_seines", "tuna_purse_seines", "set_longlines")
# turn to factor the categorical variables that I need in the model
vars_to_factor <- c("gear", "ais_type", "foc", "source_id",
                    "known_offender")
# remove variables not needed at all
vars_remove <- c("shipname", "registry_shipname", "imo", "ais_imo", "callsign",
                 "ais_callsign",
                 "positions", "overlap_hours_multinames",
                 "on_fishing_list_best",
                 "iuu", "number_iuu_encounters",
                 "fl_abuse_of_vulnerability", "fl_deception",
                 "fl_restriction_of_movement", "fl_isolation",
                 "fl_physical_and_sexual_violence",
                 "fl_intimidation_and_threats",
                 "fl_retention_of_identity_documents",
                 "fl_withholding_of_wages",
                 "fl_debt_bondage", "fl_abusive_working_and_living_conditions",
                 "fl_excessive_overtime", "fl_trafficking")

### before any filtering, we need to preprocess the data,
# particularly because we will switch some gears manually

# files.sources = list.files("./R/")
# sapply(files.sources, source)
source("./R/ml_data_prep.r")

data_preprocess <- ml_prep_data(fl_data = fl_df$data,
                                             tidy_data = alltogether_tidy$data,
                                             gears_interest, vars_to_factor,
                                             vars_remove)

training_df <- data_preprocess$training_set |>
  dplyr::select(-all_of(vars_remove)) |>
  dplyr::mutate(gear = dplyr::if_else(
    as.character(.data$gear) == "tuna_purse_seines",
    "purse_seines", as.character(.data$gear))) |>
  dplyr::filter(.data$gear != "set_longlines") |>
  dplyr::mutate(gear = as.factor(.data$gear)) |>
  dplyr::mutate(known_offender = as.factor(.data$known_offender))
# # A tibble: 4 × 2
# gear                   n
# <fct>              <int>
#   1 drifting_longlines    40
# 2 purse_seines           7
# 3 squid_jigger          14
# 4 trawlers              11

# remove: year_focus, num_years, past_ais_year, fl_event_id, possible_offender
training_df <- training_df |>
  dplyr::select(-year_focus, -num_years, -past_ais_year, -fl_event_id, -possible_offender, -event_ais_year) |>
  dplyr::relocate(where(is.numeric), .after = where(is.character)) |>
  dplyr::relocate(year, .after = ssvid) |>
  dplyr::relocate(indID, .after = year) |>
  dplyr::relocate(known_offender, .after = indID) |>
  dplyr::relocate(source_id, .after = known_offender) |>
  dplyr::relocate(known_non_offender, .after = number_voyages) |>
  dplyr::mutate(known_non_offender = as.factor(known_non_offender))

# add flag region
Asia <- c("CHN", "JPN", "KOR", "MYS", "THA", "TWN", "RUS") # Vessels we have for now (someone should double-check!!!)
training_df$flag_region <- "Other"
training_df$flag_region[which(training_df$flag %in% Asia)] <- "Asia"

head(training_df)
names(training_df)
# dir.create("./outputs")
# readr::write_rds(x = training_df, file = "./outputs/training.rds")

# negative cases
prediction_df <- data_preprocess$holdout_set |>
  dplyr::mutate(gear = dplyr::if_else(
    as.character(.data$gear) == "tuna_purse_seines",
    "purse_seines", as.character(.data$gear))) |>
  dplyr::filter(.data$gear != "set_longlines") |>
  dplyr::mutate(gear = as.factor(.data$gear)) |>
  dplyr::mutate(known_offender = as.factor(known_offender)) |>
  dplyr::filter(known_non_offender == 1 & event_ais_year == 1) |>
  dplyr::select(-year_focus, -num_years, -past_ais_year, -fl_event_id, -possible_offender, -event_ais_year) |>
  dplyr::relocate(where(is.numeric), .after = where(is.character)) |>
  dplyr::relocate(year, .after = ssvid) |>
  dplyr::relocate(indID, .after = year) |>
  dplyr::relocate(known_offender, .after = indID) |>
  dplyr::relocate(source_id, .after = known_offender) |>
  dplyr::relocate(known_non_offender, .after = number_voyages) |>
  dplyr::mutate(known_non_offender = as.factor(known_non_offender))
prediction_df$flag_region <- "Other"
prediction_df$flag_region[which(prediction_df$flag %in% Asia)] <- "Asia"

# readr::write_rds(x = prediction_df, file = "./outputs/prediction.rds")

