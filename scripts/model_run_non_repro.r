
training_df <- readr::read_rds(file = "./outputs/training.rds")
prediction_df <- readr::read_rds(file = "./outputs/prediction.rds")
# col_factor <- c("known_offender", "source_id", "gear", "ais_type", "foc")
# training_repro[col_factor] <- lapply(training_repro[col_factor], factor)
whole_df <- rbind.data.frame(training_df, prediction_df)

########## GENERATING TABLE 1 #############

whole_df |>
  dplyr::mutate(case_type =
                  dplyr::case_when(known_offender == 1 ~ "Positive",
                                   known_offender == 0 & known_non_offender == 0 ~ "Unlabeled",
                                   known_non_offender == 1 ~ "Negative")) |>
  dplyr::group_by(case_type) |>
  dplyr::summarise(n = dplyr::n())

# # A tibble: 3 × 2
# case_type      n
# <chr>      <int>
#   1 Negative      53
# 2 Positive      72
# 3 Unlabeled 107256



########## GENERATING TABLE 2 #############

whole_df |>
  dplyr::mutate(case_type =
                  dplyr::case_when(known_offender == 1 ~ "Positive",
                                   known_offender == 0 & known_non_offender == 0 ~ "Unlabeled",
                                   known_non_offender == 1 ~ "Negative")) |>
  dplyr::group_by(gear, flag_region, case_type) |>
  dplyr::summarise(n = dplyr::n())

# # A tibble: 16 × 4
# # Groups:   gear, flag_region [8]
# gear               flag_region case_type     n
# <fct>              <chr>       <chr>     <int>
#   1 drifting_longlines Asia        Positive     30
# 2 drifting_longlines Asia        Unlabeled 12850
# 3 drifting_longlines Other       Positive     10
# 4 drifting_longlines Other       Unlabeled  8037
# 5 purse_seines       Asia        Positive      6
# 6 purse_seines       Asia        Unlabeled  1078
# 7 purse_seines       Other       Positive      1
# 8 purse_seines       Other       Unlabeled  4232
# 9 squid_jigger       Asia        Positive     14
# 10 squid_jigger       Asia        Unlabeled  5221
# 11 squid_jigger       Other       Unlabeled   801
# 12 trawlers           Asia        Positive      9
# 13 trawlers           Asia        Unlabeled  6150
# 14 trawlers           Other       Negative     53
# 15 trawlers           Other       Positive      2
# 16 trawlers           Other       Unlabeled 68887



############### NOW MAKING SURE THAT THE MODEL RUNS WITH THIS FORMAT OF #####
######################## TRAINING DATA #######################################

################## writing the recipe ##########################################

fl_rec <- recipes::recipe(known_offender ~ .,
                          # modeling known_offender using everything else
                          data = training_df) |>
  # actually some are more id variables
  recipes::update_role(indID,
                       new_role = "id") |>
  # actually I don't want to use other variables in the model
  recipes::update_role(flag_region, ssvid, year, flag, known_non_offender,
                       new_role = "dont_use")  |>
  # and some others will be useful for preventing data leakage
  recipes::update_role(source_id, new_role = "control")  |>
  # Remove near-zero variance numeric predictors
  recipes::step_nzv(recipes::all_predictors())  |>
  # almost zero variance removal (I don't think we have those though)
  # Remove numeric predictors that have correlation greater the 75%
  recipes::step_corr(recipes::all_numeric(), threshold = 0.75)



######### specifying the model #################################################

# RF with hyperparameters to tune
rf_spec <-
  # type of model # if no tuning # rand_forest()
  parsnip::rand_forest(trees = 500,
                       # We will tune these two hyperparameters
                       mtry = tune(),
                       min_n = tune()) |>
  # mode
  parsnip::set_mode("classification") |>
  # engine/package
  parsnip::set_engine("ranger", regularization.factor = tune())



########### training and testing scheme ########################################

## defining some parameter values ##
num_folds <- 2 # number of folds SHOULD BE 5
num_bags <- 5 #10,20,30,50,100 # Keep this low for now for speed,
# but can crank up later
down_sample_ratio <- 1 # downsampling ratio
# Set common seed to use anywhere that uses random numbers
# We'll vary this to get confidence intervals
# Eventually we can crank this up (16,32,64), but keep it to 2 for now for
# testing
num_common_seeds <- 2
common_seed_tibble <- tibble::tibble(common_seed =
                                       seq(1:num_common_seeds) * 101)


# Run all common_seeds
bag_runs <- common_seed_tibble |>
  # For each common_seed, run all bags
  tidyr::crossing(tibble::tibble(bag = seq(num_bags))) |>
  # Will use different random seeds when implementing recipes for each bag
  dplyr::mutate(recipe_seed = dplyr::row_number() * common_seed) |>
  # counter
  dplyr::mutate(counter = dplyr::row_number())

## parallelization strategy
parallel_plan <- "multicore" # multisession if running from RStudio, or
# multicore if from Linux, Mac and plain R, or
# psock if multisession is not working well and you need to try something else
free_cores <- 1 # add more if you need to do many things at the same time


## CROSS VALIDATION ##
# Ensure there is no splitting across source_id across analysis and assessment
# data sets.  Need to make separate splits for each seed.
cv_splits_all <- common_seed_tibble |>
  dplyr::mutate(cv_splits = purrr::map(common_seed, function(x) {
    set.seed(x)
    rsample::group_vfold_cv(training_df,
                            group = source_id,
                            v = num_folds)
  }))




### installing and loading package ###

if (!require("forcedlabor")) {
  credentials::set_github_pat()
  devtools::install_github("GlobalFishingWatch/forcedlabor@main")
}


### FIRST TRAINING STAGE ###

tictoc::tic()
train_pred_proba <- forcedlabor::ml_training(training_df = training_df,
                                             fl_rec = fl_rec,
                                             rf_spec = rf_spec,
                                             cv_splits_all = cv_splits_all,
                                             bag_runs = bag_runs,
                                             down_sample_ratio = down_sample_ratio,
                                             num_grid = 2,
                                             parallel_plan = parallel_plan,
                                             free_cores = free_cores)
tictoc::toc()



###### finding the optimal threshold and hyperparameters #########

tictoc::tic()
best_hyperparameters <- forcedlabor::ml_hyperpar(train_pred_proba)
# write_csv(best_hyperparameters,here::here("outputs/stats",
# "best_hyperpar.csv"))
tictoc::toc()



####### Frankenstraining ########################################

tictoc::tic()
cv_model_res <- forcedlabor::ml_frankenstraining(training_df = training_df,
                                                 fl_rec = fl_rec,
                                                 rf_spec = rf_spec,
                                                 cv_splits_all = cv_splits_all,
                                                 bag_runs = bag_runs,
                                                 down_sample_ratio = down_sample_ratio,
                                                 parallel_plan = parallel_plan,
                                                 free_cores = free_cores,
                                                 best_hyperparameters = best_hyperparameters,
                                                 prediction_df = prediction_df)
tictoc::toc()


####### Classification with dedpul ########################################
# alpha value will be printed

tictoc::tic()
classif_res <- forcedlabor::ml_classification(data = cv_model_res, common_seed_tibble,
                                              steps = 1000, plotting = FALSE,
                                              filepath = NULL,
                                              threshold = seq(0, .99, by = 0.01), eps = 0.01)
tictoc::toc()


########### Recall and specificity #################################################

tictoc::tic()
perf_metrics <- forcedlabor::ml_perf_metrics(data = classif_res)
tictoc::toc()
#
# > perf_metrics
# recall specif
# 1 0.8888889      1



########### Predictions ############################


predictions <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(prediction) |>
  dplyr::summarise(N = dplyr::n())

# > predictions
# # A tibble: 2 × 2
# prediction     N
# <chr>      <int>
#   1 Negative   78656
# 2 Positive   28725


classif_res <- classif_res |>
  dplyr::left_join(whole_df, by = c("indID", "known_offender", "known_non_offender")) |>
  dplyr::select(indID, known_offender, known_non_offender, pred_class, confidence, gear, flag_region)



# Predictions by gear

predictions_gear <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(prediction, gear) |>
  dplyr::summarise(N = dplyr::n())

# # A tibble: 8 × 3
# # Groups:   prediction [2]
# prediction gear                   N
# <chr>      <fct>              <int>
#   1 Negative   drifting_longlines  5056
# 2 Negative   purse_seines        1979
# 3 Negative   squid_jigger         362
# 4 Negative   trawlers           71259
# 5 Positive   drifting_longlines 15871
# 6 Positive   purse_seines        3338
# 7 Positive   squid_jigger        5674
# 8 Positive   trawlers            3842

# Predictions by region

predictions_region <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(prediction, flag_region) |>
  dplyr::summarise(N = dplyr::n())

# # A tibble: 4 × 3
# # Groups:   prediction [2]
# prediction flag_region     N
# <chr>      <chr>       <int>
#   1 Negative   Asia         5980
# 2 Negative   Other       72676
# 3 Positive   Asia        19378
# 4 Positive   Other        9347


# Predictions by gear-region

predictions_gear_region <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(prediction, gear, flag_region) |>
  dplyr::summarise(N = dplyr::n())

# # A tibble: 16 × 4
# # Groups:   prediction, gear [8]
# prediction gear               flag_region     N
# <chr>      <fct>              <chr>       <int>
#   1 Negative   drifting_longlines Asia         1802
# 2 Negative   drifting_longlines Other        3254
# 3 Negative   purse_seines       Asia          124
# 4 Negative   purse_seines       Other        1855
# 5 Negative   squid_jigger       Asia          100
# 6 Negative   squid_jigger       Other         262
# 7 Negative   trawlers           Asia         3954
# 8 Negative   trawlers           Other       67305
# 9 Positive   drifting_longlines Asia        11078
# 10 Positive   drifting_longlines Other        4793
# 11 Positive   purse_seines       Asia          960
# 12 Positive   purse_seines       Other        2378
# 13 Positive   squid_jigger       Asia         5135
# 14 Positive   squid_jigger       Other         539
# 15 Positive   trawlers           Asia         2205
# 16 Positive   trawlers           Other        1637


########### Confidence levels ##########################


count_conf <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(.data$prediction) |>
  dplyr::filter(confidence > 0.8) |>
  dplyr::summarise(n = dplyr::n())

predictions |> dplyr::left_join(count_conf, by = "prediction") |>
  dplyr::mutate(prop = n/N)

# # A tibble: 2 × 4
# prediction     N     n  prop
# <chr>      <int> <int> <dbl>
#   1 Negative   78656 71696 0.912
# 2 Positive   28725 21716 0.756



classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(.data$prediction, .data$gear) |>
  dplyr::filter(confidence > 0.8) |>
  dplyr::summarise(n = dplyr::n()) |>
  dplyr::left_join(predictions_gear, by = c("prediction", "gear")) |>
  dplyr::mutate(prop = n/N)

# # A tibble: 8 × 5
# # Groups:   prediction [2]
# prediction gear                   n     N  prop
# <chr>      <fct>              <int> <int> <dbl>
#   1 Negative   drifting_longlines  2908  5056 0.575
# 2 Negative   purse_seines        1630  1979 0.824
# 3 Negative   squid_jigger         111   362 0.307
# 4 Negative   trawlers           67047 71259 0.941
# 5 Positive   drifting_longlines 12314 15871 0.776
# 6 Positive   purse_seines        2638  3338 0.790
# 7 Positive   squid_jigger        5261  5674 0.927
# 8 Positive   trawlers            1503  3842 0.391


classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(.data$prediction, .data$flag_region) |>
  dplyr::filter(confidence > 0.8) |>
  dplyr::summarise(n = dplyr::n()) |>
  dplyr::left_join(predictions_region, by = c("prediction", "flag_region")) |>
  dplyr::mutate(prop = n/N)

# # A tibble: 4 × 5
# # Groups:   prediction [2]
# prediction flag_region     n     N  prop
# <chr>      <chr>       <int> <int> <dbl>
#   1 Negative   Asia         3608  5980 0.603
# 2 Negative   Other       68088 72676 0.937
# 3 Positive   Asia        15525 19378 0.801
# 4 Positive   Other        6191  9347 0.662


classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(.data$prediction, .data$gear, .data$flag_region) |>
  dplyr::filter(confidence > 0.8) |>
  dplyr::summarise(n = dplyr::n()) |>
  dplyr::left_join(predictions_gear_region, by = c("prediction", "gear", "flag_region")) |>
  dplyr::mutate(prop = n/N)

# # A tibble: 16 × 6
# # Groups:   prediction, gear [8]
# prediction gear               flag_region     n     N  prop
# <chr>      <fct>              <chr>       <int> <int> <dbl>
#   1 Negative   drifting_longlines Asia          597  1802 0.331
# 2 Negative   drifting_longlines Other        2311  3254 0.710
# 3 Negative   purse_seines       Asia           94   124 0.758
# 4 Negative   purse_seines       Other        1536  1855 0.828
# 5 Negative   squid_jigger       Asia           59   100 0.59
# 6 Negative   squid_jigger       Other          52   262 0.198
# 7 Negative   trawlers           Asia         2858  3954 0.723
# 8 Negative   trawlers           Other       64189 67305 0.954
# 9 Positive   drifting_longlines Asia         8997 11078 0.812
# 10 Positive   drifting_longlines Other        3317  4793 0.692
# 11 Positive   purse_seines       Asia          726   960 0.756
# 12 Positive   purse_seines       Other        1912  2378 0.804
# 13 Positive   squid_jigger       Asia         5032  5135 0.980
# 14 Positive   squid_jigger       Other         229   539 0.425
# 15 Positive   trawlers           Asia          770  2205 0.349
# 16 Positive   trawlers           Other         733  1637 0.448


######### Fairness (recall) ########################################

# by gear

classif_res |>
  dplyr::group_by(gear) |>
  yardstick::recall(truth = factor(.data$known_offender,
                                   levels = c(1, 0)),
                    estimate = factor(.data$pred_class,
                                      levels = c(1, 0))) |>
  dplyr::select(gear, .data$.estimate)


# # A tibble: 4 × 2
# gear               .estimate
# <fct>                  <dbl>
#   1 drifting_longlines     1
# 2 purse_seines           0.857
# 3 squid_jigger           1
# 4 trawlers               0.364


# by flag_region

classif_res |>
  dplyr::group_by(flag_region) |>
  yardstick::recall(truth = factor(.data$known_offender,
                                   levels = c(1, 0)),
                    estimate = factor(.data$pred_class,
                                      levels = c(1, 0))) |>
  dplyr::select(flag_region, .data$.estimate)

# # A tibble: 2 × 2
# flag_region .estimate
# <chr>           <dbl>
#   1 Asia            0.898
# 2 Other           0.846


# by gear and flag_region

classif_res |>
  dplyr::group_by(gear, flag_region) |>
  yardstick::recall(truth = factor(.data$known_offender,
                                   levels = c(1, 0)),
                    estimate = factor(.data$pred_class,
                                      levels = c(1, 0))) |>
  dplyr::select(gear, flag_region, .data$.estimate)
#
# # A tibble: 8 × 3
# gear               flag_region .estimate
# <fct>              <chr>           <dbl>
#   1 drifting_longlines Asia            1
# 2 drifting_longlines Other           1
# 3 purse_seines       Asia            0.833
# 4 purse_seines       Other           1
# 5 squid_jigger       Asia            1
# 6 squid_jigger       Other          NA
# 7 trawlers           Asia            0.444
# 8 trawlers           Other           0
