
training_df <- readr::read_rds(file = "./outputs/training.rds")

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

# RF with hyperparameters based on an exercise with 5-fold cv, 30 bags, 5 seeds
# and 5-grid tunning.
rf_spec <-
  # type of model # if no tuning # rand_forest()
  parsnip::rand_forest(trees = 500,
                       # We will tune these two hyperparameters
                       mtry = 3,
                       min_n = 20) |>
  # mode
  parsnip::set_mode("classification") |>
  # engine/package
  parsnip::set_engine("ranger", regularization.factor = 0.788)



########### training and testing scheme ########################################

## defining some parameter values ##
num_folds <- 5 # number of folds
down_sample_ratio <- 1 # downsampling ratio
# Set common seed to use anywhere that uses random numbers
num_common_seeds <- 1
common_seed_tibble <- tibble::tibble(common_seed =
                                       seq(1:num_common_seeds) * 101)

## parallelization strategy
parallel_plan <- "multicore" # multisession if running from RStudio, or
# multicore if from Linux, Mac and plain R, or
# psock if multisession is not working well and you need to try something else
if (parallel_plan == "multisession"){
  utils::globalVariables("multisession")
}
free_cores <- 1 # add more if you need to do many things at the same time



### installing and loading package ###

if (!require("forcedlabor")) {
  credentials::set_github_pat()
  devtools::install_github("GlobalFishingWatch/forcedlabor@main")
}



####### STARTING WITH 30 BAGS #########################################

num_bags <- 30

# Run all common_seeds
bag_runs <- common_seed_tibble |>
  # For each common_seed, run all bags
  tidyr::crossing(tibble::tibble(bag = seq(num_bags))) |>
  # Will use different random seeds when implementing recipes for each bag
  dplyr::mutate(recipe_seed = dplyr::row_number() * common_seed) |>
  # counter
  dplyr::mutate(counter = dplyr::row_number())

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

### FIRST TRAINING STAGE ###
# tictoc::tic()
# train_pred_proba_2 <- forcedlabor::ml_training_fixedrf(training_df = training_df,
#                                              fl_rec = fl_rec,
#                                              rf_spec = rf_spec,
#                                              cv_splits_all = cv_splits_all,
#                                              bag_runs = bag_runs,
#                                              down_sample_ratio = down_sample_ratio,
#                                              parallel_plan = parallel_plan,
#                                              free_cores = free_cores)
# tictoc::toc()

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

###### AUC ########

# Starting with 1 30-bags model

roc_auc_results <- train_pred_proba |>
  yardstick::roc_auc(truth = known_offender,
                     .pred_1) |>
  dplyr::select(.estimate) |>
  purrr::pluck(1) # unlist first (unique) element


# > roc_auc_results
# [1] 0.8544894


group_auc <- function(train_pred_proba_2, groups){
  as.numeric(unlist(purrr::map_dfr(groups, function(x){
    train_pred_proba_2 |>
      dplyr::filter(bag %in% x) |>
      yardstick::roc_auc(truth = .data$known_offender,
                         .data$.pred_1) |>
      dplyr::select(.data$.estimate)
  })))
}

# 2 groups of 15
# we just need to separate into two groups: 1-15 and 16-30
groups <- list(1:15, 16:30)
auc_2_15 <- group_auc(train_pred_proba, groups)
# > auc_2_15
# [1] 0.8538316 0.8551524


# 3 groups of 10
groups <- list(1:10, 11:20, 21:30)
auc_3_10 <- group_auc(train_pred_proba, groups)
# auc_3_10
# [1] 0.8538282 0.8541214 0.8555233


# 6 groups of 5
groups <- list(1:5, 6:10, 11:15, 16:20, 21:25, 26:30)
auc_6_5 <- group_auc(train_pred_proba, groups)
# auc_6_5
# [1] 0.8517673 0.8559536 0.8538737 0.8544542 0.8587196 0.8524787


# 30 groups of 1
groups <- as.list(1:30)
auc_30_1 <- group_auc(train_pred_proba, groups)
# auc_30_1
# [1] 0.8469898 0.8487646 0.8517836 0.8560483 0.8566976 0.8513113 0.8549940
# [8] 0.8598723 0.8606106 0.8538283 0.8479835 0.8536269 0.8475765 0.8576011
# [15] 0.8656511 0.8567254 0.8488735 0.8534738 0.8581791 0.8561511 0.8521282
# [22] 0.8624326 0.8663652 0.8508699 0.8630147 0.8537562 0.8584319 0.8539573
# [29] 0.8442653 0.8552314


# stats

std <- function(x, na.rm = FALSE) {
  if (na.rm) x <- na.omit(x)
  sqrt(var(x) / length(x))
}

stats_group <- function(auc_vector, roc_auc_results ){
  # diff with whole composite set
  dif_auc <- auc_vector - rep(roc_auc_results, length(auc_30_1))
  mean_dif_auc <- mean(dif_auc)
  std_dif_auc <- std(dif_auc)

  data.frame(min = min(dif_auc), avg_minus_std_auc = mean_dif_auc - std_dif_auc,
             mean = mean_dif_auc, avg_plus_std_auc = mean_dif_auc + std_dif_auc,
             max = max(dif_auc))
}

stats_plot <- rbind.data.frame(stats_group(auc_vector = auc_30_1, roc_auc_results),
                 stats_group(auc_vector = auc_6_5, roc_auc_results),
                 stats_group(auc_vector = auc_3_10, roc_auc_results),
                 stats_group(auc_vector = auc_2_15, roc_auc_results)) |>
  dplyr::mutate(bags = c(1, 5, 10, 15))

# saveRDS(object = stats_plot, file = 'outputs/stats_plot.rds')
# stats_plot <- readRDS(file = 'outputs/stats_plot.rds')

ggplot2::ggplot(data = stats_plot, ggplot2::aes(x = bags, y = mean)) +
  ggplot2::geom_point() +
  ggplot2::geom_errorbar(ggplot2::aes(ymin = min, ymax = max), width = 0.3, linetype = 2) +
  ggplot2::geom_errorbar(ggplot2::aes(ymin = avg_minus_std_auc, ymax = avg_plus_std_auc), width = 0.3) +
  ggplot2::scale_x_continuous(breaks = c(1, 5, 10, 15))+
  ggplot2::ylab(bquote(Delta~" AUC")) +
  ggplot2::theme_bw()
ggplot2::ggsave(file = "outputs/figures/bag_sensitivity.png")

