
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
  recipes::update_role(flag_region,
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
num_folds <- 2 # number of folds
num_bags <- 5 #10
down_sample_ratio <- 1 # downsampling ratio
# Set common seed to use anywhere that uses random numbers
num_common_seeds <- 2 # 5
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

### FIRST TRAINING STAGE ###
## This stage builds models using each seed, CV analysis split, and bag
## and generates predictions for CV assessment split, which can later be evaluated against the observed classes
## Each model uses the data pre-processing recipe and model as specified above

tictoc::tic()
# train_pred_proba <- forcedlabor::ml_training(training_df = training_repro,
#                                              fl_rec = fl_rec,
#                                              rf_spec = rf_spec,
#                                              cv_splits_all = cv_splits_all,
#                                              bag_runs = bag_runs,
#                                              down_sample_ratio = down_sample_ratio,
#                                              num_grid = 2,
#                                              parallel_plan = parallel_plan,
#                                              free_cores = free_cores)
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



###### finding the optimal threshold and hyperparameters #########
## This stage selects the best hyperparameter combination from train_pred_proba
## by maximizing mean ROC AUC, averaged across folds, as the metric

tictoc::tic()
best_hyperparameters <- forcedlabor::ml_hyperpar(train_pred_proba)
# write_csv(best_hyperparameters,here::here("outputs/stats",
# "best_hyperpar.csv"))
tictoc::toc()



####### Frankenstraining ########################################
## Using the best hyperparameters, generate predictions for each random seed, CV split, and bag

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
                                                 prediction_df = NULL)
tictoc::toc()


####### Classification with dedpul ########################################
## Using the predictions for each random seed, CV split, and bag,
## use the dedpul algorithm to determine a cutoff for predicting positive or negative
## then use that cutoff to classify each observation
## This also generates a confidence value for each observation
## alpha value will be printed


tictoc::tic()
classif_res <- forcedlabor::ml_classification(data = cv_model_res, common_seed_tibble,
                                              steps = 1000, plotting = FALSE,
                                              filepath = NULL,
                                              threshold = seq(0, .99, by = 0.01), eps = 0.01)
tictoc::toc()


########### Recall #################################################
## Calculate recall using the predictions
recall_res <-  forcedlabor::ml_recall(data = classif_res)
# > recall_res
# [1] 0.9444444



########### Predictions ############################
## Summarize the predictions

predictions <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(prediction) |>
  dplyr::summarise(N = dplyr::n())

#  prediction     n
#  <chr>      <int>
#1 Negative   73182
#2 Positive   34146



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
  ungroup()

# Predictions by region

predictions_region <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(prediction, flag_region) |>
  dplyr::summarise(N = dplyr::n()) |>
  ungroup()

# Predictions by gear-region

predictions_gear_region <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(prediction, gear, flag_region) |>
  dplyr::summarise(N = dplyr::n()) |>
  ungroup()


########### Confidence levels ##########################

## Summarize prediction classes where we have at least 80% confidence
count_conf <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(.data$prediction) |>
  dplyr::filter(confidence > 0.8) |>
  dplyr::summarise(n = dplyr::n()) |>
  ungroup()

predictions |>
  dplyr::left_join(count_conf, by = "prediction") |>
  dplyr::mutate(prop = n/N)

## A tibble: 2 × 4
#  prediction     N     n  prop
#  <chr>      <int> <int> <dbl>
#1 Negative   73182 64350 0.879
#2 Positive   34146 25300 0.741


## Summarize prediction classes, by gear, where we have at least 80% confidence
classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(.data$prediction, .data$gear) |>
  dplyr::filter(confidence > 0.8) |>
  dplyr::summarise(n = dplyr::n()) |>
  ungroup() |>
  dplyr::left_join(predictions_gear, by = c("prediction", "gear")) |>
  dplyr::mutate(prop = n/N)

## A tibble: 8 × 5
## Groups:   prediction [2]
#  prediction gear                   n     N  prop
#  <chr>      <fct>              <int> <int> <dbl>
#1 Negative   drifting_longlines  2070  3487 0.594
#2 Negative   purse_seines        1363  1683 0.810
#3 Negative   squid_jigger          37   154 0.240
#4 Negative   trawlers           60880 67858 0.897
#5 Positive   drifting_longlines 13697 17440 0.785
#6 Positive   purse_seines        3170  3634 0.872
#7 Positive   squid_jigger        5529  5882 0.940
#8 Positive   trawlers            2904  7190 0.404


## Summarize prediction classes, by region, where we have at least 80% confidence
classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(.data$prediction, .data$flag_region) |>
  dplyr::filter(confidence > 0.8) |>
  dplyr::summarise(n = dplyr::n()) |>
  ungroup() |>
  dplyr::left_join(predictions_region, by = c("prediction", "flag_region")) |>
  dplyr::mutate(prop = n/N)

## A tibble: 4 × 5
## Groups:   prediction [2]
#  prediction flag_region     n     N  prop
#  <chr>      <chr>       <int> <int> <dbl>
#1 Negative   Asia         2709  4006 0.676
#2 Negative   Other       61641 69176 0.891
#3 Positive   Asia        17406 21352 0.815
#4 Positive   Other        7894 12794 0.617

## Summarize prediction classes, by gear and region, where we have at least 80% confidence
classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(.data$prediction, .data$gear, .data$flag_region) |>
  dplyr::filter(confidence > 0.8) |>
  dplyr::summarise(n = dplyr::n()) |>
  ungroup() |>
  dplyr::left_join(predictions_gear_region, by = c("prediction", "gear", "flag_region")) |>
  dplyr::mutate(prop = n/N)

## A tibble: 16 × 6
## Groups:   prediction, gear [8]
#   prediction gear               flag_region     n     N  prop
#   <chr>      <fct>              <chr>       <int> <int> <dbl>
# 1 Negative   drifting_longlines Asia          294   892 0.330
# 2 Negative   drifting_longlines Other        1776  2595 0.684
# 3 Negative   purse_seines       Asia           79    89 0.888
# 4 Negative   purse_seines       Other        1284  1594 0.806
# 5 Negative   squid_jigger       Asia           22    63 0.349
# 6 Negative   squid_jigger       Other          15    91 0.165
# 7 Negative   trawlers           Asia         2314  2962 0.781
# 8 Negative   trawlers           Other       58566 64896 0.902
# 9 Positive   drifting_longlines Asia         9785 11988 0.816
#10 Positive   drifting_longlines Other        3912  5452 0.718
#11 Positive   purse_seines       Asia          897   995 0.902
#12 Positive   purse_seines       Other        2273  2639 0.861
#13 Positive   squid_jigger       Asia         5115  5172 0.989
#14 Positive   squid_jigger       Other         414   710 0.583
#15 Positive   trawlers           Asia         1609  3197 0.503
#16 Positive   trawlers           Other        1295  3993 0.324


######### Fairness (recall) ########################################

# by gear

classif_res |>
  dplyr::group_by(gear) |>
  yardstick::recall(truth = factor(.data$known_offender,
                                   levels = c(1, 0)),
                    estimate = factor(.data$pred_class,
                                      levels = c(1, 0)))  |>
  ungroup() |>
  dplyr::select(gear, .data$.estimate)

## A tibble: 4 × 2
#  gear               .estimate
#  <fct>                  <dbl>
#1 drifting_longlines     1
#2 purse_seines           1
#3 squid_jigger           1
#4 trawlers               0.636


# by flag_region

classif_res |>
  dplyr::group_by(flag_region) |>
  yardstick::recall(truth = factor(.data$known_offender,
                                   levels = c(1, 0)),
                    estimate = factor(.data$pred_class,
                                      levels = c(1, 0))) |>
  ungroup()  |>
  dplyr::select(flag_region, .data$.estimate)

# # A tibble: 2 × 2
#  flag_region .estimate
#  <chr>           <dbl>
#1 Asia            0.949
#2 Other           0.923

# by gear and flag_region

classif_res |>
  dplyr::group_by(gear, flag_region) |>
  yardstick::recall(truth = factor(.data$known_offender,
                                   levels = c(1, 0)),
                    estimate = factor(.data$pred_class,
                                      levels = c(1, 0))) |>
  ungroup() |>
  dplyr::select(gear, flag_region, .data$.estimate)

## A tibble: 8 × 3
#  gear               flag_region .estimate
#  <fct>              <chr>           <dbl>
#1 drifting_longlines Asia            1
#2 drifting_longlines Other           1
#3 purse_seines       Asia            1
#4 purse_seines       Other           1
#5 squid_jigger       Asia            1
#6 squid_jigger       Other          NA
#7 trawlers           Asia            0.667
#8 trawlers           Other           0.5



