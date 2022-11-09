
training_df <- readr::read_rds(file = "./outputs/training.rds")
# we don't train with known_offenders, so it doesn't have level 1 in known_non_offender
levels(training_df$known_non_offender) <- c(levels(training_df$known_non_offender),1)

prediction_df <- readr::read_rds(file = "./outputs/prediction.rds")
# we only have known_offenders in the prediction dataset, so it doesn't have level 0 in known_non_offender
levels(prediction_df$known_non_offender) <- c(levels(prediction_df$known_non_offender),0)

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

# RF with hyperparameters based on sensitivity_hyper.r results
rf_spec <-
  # type of model # if no tuning # rand_forest()
  parsnip::rand_forest(trees = 500,
                       # We will tune these two hyperparameters
                       mtry = 1,
                       min_n = 15) |>
  # mode
  parsnip::set_mode("classification") |>
  # engine/package
  parsnip::set_engine("ranger", regularization.factor = 0.5)



########### training and testing scheme ########################################

## defining some parameter values based on sensitivity results
num_folds <- 5
num_bags <- 10
down_sample_ratio <- 1 # downsampling ratio
# Set common seed to use anywhere that uses random numbers
num_common_seeds <- 3
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
if (parallel_plan == "multisession"){
  utils::globalVariables("multisession")
}
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


### training and prediction (scores)

tictoc::tic()
train_pred_proba <- forcedlabor::ml_train_predict(training_df = training_df,
                                             fl_rec = fl_rec,
                                             rf_spec = rf_spec,
                                             cv_splits_all = cv_splits_all,
                                             bag_runs = bag_runs,
                                             down_sample_ratio = down_sample_ratio,
                                             parallel_plan = parallel_plan,
                                             free_cores = free_cores,
                                             prediction_df = prediction_df)
tictoc::toc()


####### Classification with dedpul ########################################
# alpha value will be printed

tictoc::tic()
classif_res <- forcedlabor::ml_classification(data = train_pred_proba,
                                              steps = 1000, plotting = FALSE,
                                              filepath = NULL,
                                              threshold = seq(0, .99, by = 0.01),
                                              eps = 0.01,
                                              parallel_plan = parallel_plan,
                                              free_cores = free_cores)
tictoc::toc()


########### Recall and specificity #################################################

tictoc::tic()
perf_metrics <- forcedlabor::ml_perf_metrics(data = classif_res$pred_conf)
tictoc::toc()
#
# perf_metrics
# recall    specif
# 1 0.8888889 0.9811321

alpha <- classif_res$alpha
# [1] 0.2832833

########### Predictions ############################

classif_res <- classif_res$pred_conf

predictions <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(prediction) |>
  dplyr::summarise(N = dplyr::n())

# predictions
# # A tibble: 2 × 2
# prediction     N
# <chr>      <int>
#   1 Negative   77574
# 2 Positive   29807



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
#   1 Negative   drifting_longlines  4281
# 2 Negative   purse_seines        1986
# 3 Negative   squid_jigger         436
# 4 Negative   trawlers           70871
# 5 Positive   drifting_longlines 16646
# 6 Positive   purse_seines        3331
# 7 Positive   squid_jigger        5600
# 8 Positive   trawlers            4230


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
#   1 Negative   Asia         5276
# 2 Negative   Other       72298
# 3 Positive   Asia        20082
# 4 Positive   Other        9725


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
#   1 Negative   drifting_longlines Asia         1257
# 2 Negative   drifting_longlines Other        3024
# 3 Negative   purse_seines       Asia          143
# 4 Negative   purse_seines       Other        1843
# 5 Negative   squid_jigger       Asia          126
# 6 Negative   squid_jigger       Other         310
# 7 Negative   trawlers           Asia         3750
# 8 Negative   trawlers           Other       67121
# 9 Positive   drifting_longlines Asia        11623
# 10 Positive   drifting_longlines Other        5023
# 11 Positive   purse_seines       Asia          941
# 12 Positive   purse_seines       Other        2390
# 13 Positive   squid_jigger       Asia         5109
# 14 Positive   squid_jigger       Other         491
# 15 Positive   trawlers           Asia         2409
# 16 Positive   trawlers           Other        1821


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
#   1 Negative   77574 74822 0.965
# 2 Positive   29807 26633 0.894


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
#   1 Negative   drifting_longlines  3332  4281 0.778
# 2 Negative   purse_seines        1836  1986 0.924
# 3 Negative   squid_jigger         237   436 0.544
# 4 Negative   trawlers           69417 70871 0.979
# 5 Positive   drifting_longlines 15393 16646 0.925
# 6 Positive   purse_seines        2970  3331 0.892
# 7 Positive   squid_jigger        5307  5600 0.948
# 8 Positive   trawlers            2963  4230 0.700


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
#   1 Negative   Asia         4298  5276 0.815
# 2 Negative   Other       70524 72298 0.975
# 3 Positive   Asia        18610 20082 0.927
# 4 Positive   Other        8023  9725 0.825


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
#   1 Negative   drifting_longlines Asia          806  1257 0.641
# 2 Negative   drifting_longlines Other        2526  3024 0.835
# 3 Negative   purse_seines       Asia          112   143 0.783
# 4 Negative   purse_seines       Other        1724  1843 0.935
# 5 Negative   squid_jigger       Asia           96   126 0.762
# 6 Negative   squid_jigger       Other         141   310 0.455
# 7 Negative   trawlers           Asia         3284  3750 0.876
# 8 Negative   trawlers           Other       66133 67121 0.985
# 9 Positive   drifting_longlines Asia        10957 11623 0.943
# 10 Positive   drifting_longlines Other        4436  5023 0.883
# 11 Positive   purse_seines       Asia          823   941 0.875
# 12 Positive   purse_seines       Other        2147  2390 0.898
# 13 Positive   squid_jigger       Asia         5019  5109 0.982
# 14 Positive   squid_jigger       Other         288   491 0.587
# 15 Positive   trawlers           Asia         1811  2409 0.752
# 16 Positive   trawlers           Other        1152  1821 0.633


######### Fairness (recall) ########################################

# by gear

classif_res |>
  dplyr::group_by(gear) |>
  yardstick::recall(truth = factor(known_offender,
                                   levels = c(1, 0)),
                    estimate = factor(pred_class,
                                      levels = c(1, 0))) |>
  dplyr::select(gear, .estimate)

# # A tibble: 4 × 2
# gear               .estimate
# <fct>                  <dbl>
#   1 drifting_longlines     1
# 2 purse_seines           0.714
# 3 squid_jigger           1
# 4 trawlers               0.455


# by flag_region

classif_res |>
  dplyr::group_by(flag_region) |>
  yardstick::recall(truth = factor(known_offender,
                                   levels = c(1, 0)),
                    estimate = factor(pred_class,
                                      levels = c(1, 0))) |>
  dplyr::select(flag_region, .estimate)

# # A tibble: 2 × 2
# flag_region .estimate
# <chr>           <dbl>
#   1 Asia            0.881
# 2 Other           0.923


# by gear and flag_region

classif_res |>
  dplyr::group_by(gear, flag_region) |>
  yardstick::recall(truth = factor(known_offender,
                                   levels = c(1, 0)),
                    estimate = factor(pred_class,
                                      levels = c(1, 0))) |>
  dplyr::select(gear, flag_region, .estimate)
# #
# # A tibble: 8 × 3
# gear               flag_region .estimate
# <fct>              <chr>           <dbl>
#   1 drifting_longlines Asia            1
# 2 drifting_longlines Other           1
# 3 purse_seines       Asia            0.667
# 4 purse_seines       Other           1
# 5 squid_jigger       Asia            1
# 6 squid_jigger       Other          NA
# 7 trawlers           Asia            0.444
# 8 trawlers           Other           0.5