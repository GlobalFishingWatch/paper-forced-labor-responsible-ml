## Shaping the data

# Part of this needs to be modified since the data will be added to the repo

## remotes::install_github("GlobalFishingWatch/forcedlabor@review")


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
### I NEED TO FIGURE OUT IF THIS WILL STAY IN THE PACKAGE OR IF IT MAKES SENSE TO COPY THE FUNCTION HERE
### ACTUALLY IT MAKES MORE SENSE TO DO THIS PROCESSING INTERNALLY AND ONLY GET ALL THE FINAL DATA IN ONE FILE

data_preprocess <- forcedlabor::ml_prep_data(fl_data = fl_df$data,
                                tidy_data = alltogether_tidy$data,
                                gears_interest, vars_to_factor,
                                vars_remove)
training_df <- data_preprocess$training_set

training_df <- training_df |>
  dplyr::select(-all_of(vars_remove)) |>
  dplyr::mutate(gear = dplyr::if_else(
    as.character(.data$gear) == "tuna_purse_seines",
    "purse_seines", as.character(.data$gear))) |>
  dplyr::filter(.data$gear != "set_longlines") |>
  dplyr::mutate(gear = as.factor(.data$gear)) |>
  dplyr::mutate(known_offender_2 = .data$known_offender) #%>%  # for 2000 operation
# dplyr::mutate(known_offender = dplyr::if_else(
#   .data$year == 2020, 0, as.numeric(as.character(.data$known_offender_2)))
# ) %>%
# dplyr::mutate(known_offender = as.factor(.data$known_offender))



prediction_df <- data_preprocess$holdout_set

prediction_df <- data_preprocess$holdout_set |>
  dplyr::mutate(gear = dplyr::if_else(
    as.character(.data$gear) == "tuna_purse_seines",
    "purse_seines", as.character(.data$gear))) |>
  dplyr::filter(.data$gear != "set_longlines")|>
  dplyr::mutate(gear = as.factor(.data$gear))  |>
  dplyr::mutate(known_offender_2 = .data$known_offender) #%>%  # for 2000 operation
# dplyr::mutate(known_offender = dplyr::if_else(
#   .data$year == 2020, 0, as.numeric(as.character(.data$known_offender_2)))
# ) %>%
# dplyr::mutate(known_offender = as.factor(.data$known_offender))
# readr::write_csv(prediction_df,here::here("data","holdout_df.csv"))

whole_set <- training_df |>
  rbind.data.frame(prediction_df)

positive_cases <- whole_set |>
  dplyr::filter(known_offender == 1 & event_ais_year == 1)

negative_cases <- whole_set |>
  dplyr::filter(known_non_offender == 1 & event_ais_year == 1)

suspected_cases <- whole_set |>
  dplyr::filter((known_offender == 1 & event_ais_year == 0) |
                  known_non_offender == 1 & event_ais_year == 0)

unlabeled_cases <- whole_set |>
  dplyr::filter(known_offender == 0 & known_non_offender == 0)


######### Number of vessel-years

# Number of positive cases
dim(positive_cases)[1]

# Number of negative cases
dim(negative_cases)[1]

# Number of suspected cases
dim(suspected_cases)[1]

# Number of unlabeled cases
dim(unlabeled_cases)[1]


