
training_repro <- readr::read_csv(file = "./data/training_repro.csv")
col_factor <- c("known_offender", "source_id", "gear", "ais_type", "foc")
training_repro[col_factor] <- lapply(training_repro[col_factor], factor)


########## GENERATING TABLE 1 EXCEPT FOR NEGATIVE CASES #############

training_repro |>
  dplyr::mutate(case_type =
                  dplyr::case_when(known_offender == 1 ~ "Positive",
                                   known_offender == 0 ~ "Unlabeled")) |>
  dplyr::group_by(case_type) |>
  dplyr::summarise(n = dplyr::n())


########## GENERATING TABLE 2 EXCEPT FOR NEGATIVE CASES #############

training_repro |>
  dplyr::mutate(case_type =
                  dplyr::case_when(known_offender == 1 ~ "Positive",
                                   known_offender == 0 ~ "Unlabeled")) |>
  dplyr::group_by(gear, flag_region, case_type) |>
  dplyr::summarise(n = dplyr::n())


################## writing the recipe ##########################################

fl_rec <- recipes::recipe(known_offender ~ .,
                          # modeling known_offender using everything else
                          data = training_repro) |>
  # actually some are more id variables
  recipes::update_role(indID,
                       new_role = "id") |>
  # actually I don't want to use other variables in the model
  recipes::update_role(flag_region, known_non_offender,
                       new_role = "dont_use")  |>
  # and some others will be useful for preventing data leakage
  recipes::update_role(source_id, new_role = "control")  |>
  # Remove near-zero variance numeric predictors
  recipes::step_nzv(recipes::all_predictors())  |>
  # almost zero variance removal (I don't think we have those though)
  # Remove numeric predictors that have correlation greater the 75%
  recipes::step_corr(recipes::all_numeric(), threshold = 0.75)



######### specifying the model #################################################

# RF with hyperparameters based on sensitivity results
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

## defining some parameter values ##
num_folds <- 5 # number of folds
num_bags <- 10 #10
down_sample_ratio <- 1 # downsampling ratio
# Set common seed to use anywhere that uses random numbers
num_common_seeds <- 3 # 3
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
    rsample::group_vfold_cv(training_repro,
                            group = source_id,
                            v = num_folds)
  }))




### installing and loading package ###

if (!require("forcedlabor")) {
  credentials::set_github_pat()
  devtools::install_github("GlobalFishingWatch/forcedlabor@main")
}

# tictoc::tic()

# library(forcedlabor)

### Training and prediction (scores) ###########################################
## This stage builds models using each seed, CV analysis split, and bag
## and generates predictions for CV assessment split, which can later be evaluated against the observed classes
## Each model uses the data pre-processing recipe and model as specified above

tictoc::tic()
train_pred_proba <- forcedlabor::ml_train_predict(training_df = training_df,
                                                  fl_rec = fl_rec,
                                                  rf_spec = rf_spec,
                                                  cv_splits_all = cv_splits_all,
                                                  bag_runs = bag_runs,
                                                  down_sample_ratio = down_sample_ratio,
                                                  parallel_plan = parallel_plan,
                                                  free_cores = free_cores,
                                                  prediction_df = NULL)
tictoc::toc()


####### Classification with dedpul ########################################
## Using the predictions for each random seed, CV split, and bag,
## use the dedpul algorithm to determine a cutoff for predicting positive or negative
## then use that cutoff to classify each observation
## This also generates a confidence value for each observation
## alpha value will be printed


tictoc::tic()
classif_res <- forcedlabor::ml_classification(data = train_pred_proba,
                                              steps = 1000, plotting = FALSE,
                                              filepath = NULL,
                                              threshold = seq(0, .99, by = 0.01), eps = 0.01,
                                              parallel_plan = parallel_plan, free_cores = free_cores)
tictoc::toc()


########### Recall #################################################
## Calculate recall using the predictions
recall_res <-  forcedlabor::ml_recall(data = classif_res$pred_conf)
# > recall_res
# [1] 0.9444444


alpha <- classif_res$alpha
# alpha
# [1] 0.2852853


########### Predictions ############################
## Summarize the predictions

classif_res <- classif_res$pred_conf

predictions <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(prediction) |>
  dplyr::summarise(N = dplyr::n())

# # A tibble: 2 × 2
# prediction     N
# <chr>      <int>
#   1 Negative   77581
# 2 Positive   29747




classif_res <- classif_res |>
  dplyr::left_join(training_repro, by = c("indID", "known_offender")) |>
  dplyr::select(indID, known_offender, pred_class, confidence, gear, flag_region)



# Predictions by gear

predictions_gear <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(prediction, gear) |>
  dplyr::summarise(N = dplyr::n()) |>
  dplyr::ungroup()

# Predictions by region

predictions_region <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(prediction, flag_region) |>
  dplyr::summarise(N = dplyr::n()) |>
  dplyr::ungroup()

# Predictions by gear-region

predictions_gear_region <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(prediction, gear, flag_region) |>
  dplyr::summarise(N = dplyr::n()) |>
  dplyr::ungroup()


########### Confidence levels ##########################

## Summarize prediction classes where we have at least 80% confidence
count_conf <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(.data$prediction) |>
  dplyr::filter(confidence > 0.8) |>
  dplyr::summarise(n = dplyr::n()) |>
  dplyr::ungroup()

predictions |>
  dplyr::left_join(count_conf, by = "prediction") |>
  dplyr::mutate(prop = n/N)

# # A tibble: 2 × 4
# prediction     N     n  prop
# <chr>      <int> <int> <dbl>
#   1 Negative   77581 74767 0.964
# 2 Positive   29747 26507 0.891


## Summarize prediction classes, by gear, where we have at least 80% confidence
classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(.data$prediction, .data$gear) |>
  dplyr::filter(confidence > 0.8) |>
  dplyr::summarise(n = dplyr::n()) |>
  dplyr::ungroup() |>
  dplyr::left_join(predictions_gear, by = c("prediction", "gear")) |>
  dplyr::mutate(prop = n/N)

# # A tibble: 8 × 5
# prediction gear                   n     N  prop
# <chr>      <fct>              <int> <int> <dbl>
#   1 Negative   drifting_longlines  3314  4308 0.769
# 2 Negative   purse_seines        1826  1981 0.922
# 3 Negative   squid_jigger         231   438 0.527
# 4 Negative   trawlers           69396 70854 0.979
# 5 Positive   drifting_longlines 15345 16619 0.923
# 6 Positive   purse_seines        2946  3336 0.883
# 7 Positive   squid_jigger        5316  5598 0.950
# 8 Positive   trawlers            2900  4194 0.691



## Summarize prediction classes, by region, where we have at least 80% confidence
classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(.data$prediction, .data$flag_region) |>
  dplyr::filter(confidence > 0.8) |>
  dplyr::summarise(n = dplyr::n()) |>
  dplyr::ungroup() |>
  dplyr::left_join(predictions_region, by = c("prediction", "flag_region")) |>
  dplyr::mutate(prop = n/N)

# # A tibble: 4 × 5
# prediction flag_region     n     N  prop
# <chr>      <chr>       <int> <int> <dbl>
#   1 Negative   Asia         4330  5323 0.813
# 2 Negative   Other       70437 72258 0.975
# 3 Positive   Asia        18543 20035 0.926
# 4 Positive   Other        7964  9712 0.820


## Summarize prediction classes, by gear and region, where we have at least 80% confidence
classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(.data$prediction, .data$gear, .data$flag_region) |>
  dplyr::filter(confidence > 0.8) |>
  dplyr::summarise(n = dplyr::n()) |>
  dplyr::ungroup() |>
  dplyr::left_join(predictions_gear_region, by = c("prediction", "gear", "flag_region")) |>
  dplyr::mutate(prop = n/N)

# # A tibble: 16 × 6
# prediction gear               flag_region     n     N  prop
# <chr>      <fct>              <chr>       <int> <int> <dbl>
#   1 Negative   drifting_longlines Asia          821  1274 0.644
# 2 Negative   drifting_longlines Other        2493  3034 0.822
# 3 Negative   purse_seines       Asia          112   146 0.767
# 4 Negative   purse_seines       Other        1714  1835 0.934
# 5 Negative   squid_jigger       Asia           94   133 0.707
# 6 Negative   squid_jigger       Other         137   305 0.449
# 7 Negative   trawlers           Asia         3303  3770 0.876
# 8 Negative   trawlers           Other       66093 67084 0.985
# 9 Positive   drifting_longlines Asia        10933 11606 0.942
# 10 Positive   drifting_longlines Other        4412  5013 0.880
# 11 Positive   purse_seines       Asia          814   938 0.868
# 12 Positive   purse_seines       Other        2132  2398 0.889
# 13 Positive   squid_jigger       Asia         5023  5102 0.985
# 14 Positive   squid_jigger       Other         293   496 0.591
# 15 Positive   trawlers           Asia         1773  2389 0.742
# 16 Positive   trawlers           Other        1127  1805 0.624



######### Fairness (recall) ########################################

# by gear

classif_res |>
  dplyr::group_by(gear) |>
  yardstick::recall(truth = factor(.data$known_offender,
                                   levels = c(1, 0)),
                    estimate = factor(.data$pred_class,
                                      levels = c(1, 0)))  |>
  dplyr::ungroup() |>
  dplyr::select(gear, .data$.estimate)

# # A tibble: 4 × 2
# gear               .estimate
# <fct>                  <dbl>
#   1 drifting_longlines     1
# 2 purse_seines           0.714
# 3 squid_jigger           1
# 4 trawlers               0.545



# by flag_region

classif_res |>
  dplyr::group_by(flag_region) |>
  yardstick::recall(truth = factor(.data$known_offender,
                                   levels = c(1, 0)),
                    estimate = factor(.data$pred_class,
                                      levels = c(1, 0))) |>
  dplyr::ungroup()  |>
  dplyr::select(flag_region, .data$.estimate)

# # A tibble: 2 × 2
# flag_region .estimate
# <chr>           <dbl>
#   1 Asia            0.898
# 2 Other           0.923


# by gear and flag_region

classif_res |>
  dplyr::group_by(gear, flag_region) |>
  yardstick::recall(truth = factor(.data$known_offender,
                                   levels = c(1, 0)),
                    estimate = factor(.data$pred_class,
                                      levels = c(1, 0))) |>
  dplyr::ungroup() |>
  dplyr::select(gear, flag_region, .data$.estimate)

# # A tibble: 8 × 3
# gear               flag_region .estimate
# <fct>              <chr>           <dbl>
#   1 drifting_longlines Asia            1
# 2 drifting_longlines Other           1
# 3 purse_seines       Asia            0.667
# 4 purse_seines       Other           1
# 5 squid_jigger       Asia            1
# 6 squid_jigger       Other          NA
# 7 trawlers           Asia            0.556
# 8 trawlers           Other           0.5



