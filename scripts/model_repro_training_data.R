training_repro <- readr::read_rds(file = "./data_output/training_repro.rds")
# we don't train with known_offenders, so it doesn't have level 1 in known_non_offender
training_repro$known_non_offender <- factor (training_repro$known_non_offender, levels = c(0,1))


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

### FIRST TRAINING STAGE ###
## This stage builds models using each seed, CV analysis split, and hyperparameter combination
## and generates predictions for CV assessment split, which can later be evaluated against the observed classes
## Each model uses the data pre-processing recipe and model as specified above

tictoc::tic()
train_pred_proba <- forcedlabor::ml_train_predict(fl_rec = fl_rec,
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
# recall_res
# [1] 0.8888889


alpha <- classif_res$alpha
# alpha
# [1] "alpha:  0.282282282282282"



########### Predictions ############################
## Summarize the predictions
## The difference with the paper is that these ones do not have the negative cases,
## so there are less predictions

classif_res <- classif_res$pred_conf

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
#   1 Negative   77595
# 2 Positive   29733




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

# # A tibble: 8 × 3
# prediction gear                   N
# <chr>      <fct>              <int>
#   1 Negative   drifting_longlines  4349
# 2 Negative   purse_seines        1982
# 3 Negative   squid_jigger         429
# 4 Negative   trawlers           70835
# 5 Positive   drifting_longlines 16578
# 6 Positive   purse_seines        3335
# 7 Positive   squid_jigger        5607
# 8 Positive   trawlers            4213


# Predictions by region

predictions_region <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(prediction, flag_region) |>
  dplyr::summarise(N = dplyr::n()) |>
  dplyr::ungroup()

# # A tibble: 4 × 3
# prediction flag_region     N
# <chr>      <chr>       <int>
#   1 Negative   Asia         5306
# 2 Negative   Other       72289
# 3 Positive   Asia        20052
# 4 Positive   Other        9681


# Predictions by gear-region

predictions_gear_region <- classif_res |>
  dplyr::mutate(prediction =
                  dplyr::case_when(pred_class == 1 ~ "Positive",
                                   pred_class == 0 ~ "Negative")) |>
  dplyr::group_by(prediction, gear, flag_region) |>
  dplyr::summarise(N = dplyr::n()) |>
  dplyr::ungroup()

# # A tibble: 16 × 4
# prediction gear               flag_region     N
# <chr>      <fct>              <chr>       <int>
#   1 Negative   drifting_longlines Asia         1288
# 2 Negative   drifting_longlines Other        3061
# 3 Negative   purse_seines       Asia          143
# 4 Negative   purse_seines       Other        1839
# 5 Negative   squid_jigger       Asia          128
# 6 Negative   squid_jigger       Other         301
# 7 Negative   trawlers           Asia         3747
# 8 Negative   trawlers           Other       67088
# 9 Positive   drifting_longlines Asia        11592
# 10 Positive   drifting_longlines Other        4986
# 11 Positive   purse_seines       Asia          941
# 12 Positive   purse_seines       Other        2394
# 13 Positive   squid_jigger       Asia         5107
# 14 Positive   squid_jigger       Other         500
# 15 Positive   trawlers           Asia         2412
# 16 Positive   trawlers           Other        1801


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

# # A tibble: 2 × 2
# prediction     n
# <chr>      <int>
#   1 Negative   74807
# 2 Positive   26475

predictions |>
  dplyr::left_join(count_conf, by = "prediction") |>
  dplyr::mutate(prop = n/N)

# # A tibble: 2 × 4
# prediction     N     n  prop
# <chr>      <int> <int> <dbl>
#   1 Negative   77595 74807 0.964
# 2 Positive   29733 26475 0.890



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
#   1 Negative   drifting_longlines  3359  4349 0.772
# 2 Negative   purse_seines        1824  1982 0.920
# 3 Negative   squid_jigger         231   429 0.538
# 4 Negative   trawlers           69393 70835 0.980
# 5 Positive   drifting_longlines 15297 16578 0.923
# 6 Positive   purse_seines        2953  3335 0.885
# 7 Positive   squid_jigger        5305  5607 0.946
# 8 Positive   trawlers            2920  4213 0.693




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
#   1 Negative   Asia         4310  5306 0.812
# 2 Negative   Other       70497 72289 0.975
# 3 Positive   Asia        18534 20052 0.924
# 4 Positive   Other        7941  9681 0.820



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
#   1 Negative   drifting_longlines Asia          821  1288 0.637
# 2 Negative   drifting_longlines Other        2538  3061 0.829
# 3 Negative   purse_seines       Asia          111   143 0.776
# 4 Negative   purse_seines       Other        1713  1839 0.931
# 5 Negative   squid_jigger       Asia           95   128 0.742
# 6 Negative   squid_jigger       Other         136   301 0.452
# 7 Negative   trawlers           Asia         3283  3747 0.876
# 8 Negative   trawlers           Other       66110 67088 0.985
# 9 Positive   drifting_longlines Asia        10907 11592 0.941
# 10 Positive   drifting_longlines Other        4390  4986 0.880
# 11 Positive   purse_seines       Asia          813   941 0.864
# 12 Positive   purse_seines       Other        2140  2394 0.894
# 13 Positive   squid_jigger       Asia         5016  5107 0.982
# 14 Positive   squid_jigger       Other         289   500 0.578
# 15 Positive   trawlers           Asia         1798  2412 0.745
# 16 Positive   trawlers           Other        1122  1801 0.623




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
#  1 drifting_longlines     1
# 2 purse_seines           0.714
# 3 squid_jigger           1
# 4 trawlers               0.455




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
#   1 Asia            0.881
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
# 7 trawlers           Asia            0.444
# 8 trawlers           Other           0.5



