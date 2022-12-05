
training_df <- readr::read_rds(file = "./outputs/training.rds")

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
                       mtry = tune(),
                       min_n = tune()) |>
  # mode
  parsnip::set_mode("classification") |>
  # engine/package
  parsnip::set_engine("ranger", regularization.factor = tune())



########### training and testing scheme ########################################

## defining some parameter values ##
num_folds <- 5 # number of folds SHOULD BE 5
down_sample_ratio <- 1 # downsampling ratio
# Set common seed to use anywhere that uses random numbers
num_common_seeds <- 1
common_seed_tibble <- tibble::tibble(common_seed =
                                       seq(1:num_common_seeds) * 101)
num_bags <- 10

## parallelization strategy
parallel_plan <- "multicore" # multisession if running from RStudio, or
# multicore if from Linux, Mac and plain R, or
# psock if multisession is not working well and you need to try something else
if (parallel_plan == "multisession"){
  utils::globalVariables("multisession")
}
free_cores <- 1 # add more if you need to do many things at the same time

# Run all common_seeds
bag_runs <- common_seed_tibble |>
  # For each common_seed, run all bags
  tidyr::crossing(tibble::tibble(bag = seq(num_bags))) |>
  # Will use different random seeds when implementing recipes for each bag
  dplyr::mutate(recipe_seed = dplyr::row_number() * common_seed) |>
  # counter
  dplyr::mutate(counter = dplyr::row_number())

## Cross validation ##
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

### First training stage (used to choose hyperparameters) ###

grill <- expand.grid(
  mtry = 1:10,
  min_n = seq(from = 10, to = 40, by=5),
  regularization.factor = seq(0.1, 0.9, by = 0.1)
)


tictoc::tic()
train_pred_proba <- forcedlabor::ml_training(fl_rec = fl_rec,
                                             rf_spec = rf_spec,
                                             cv_splits_all = cv_splits_all,
                                             bag_runs = bag_runs,
                                             down_sample_ratio = down_sample_ratio,
                                             num_grid = grill,
                                             parallel_plan = parallel_plan,
                                             free_cores = free_cores)
tictoc::toc()

# About grid:
# An integer denotes the number of candidate parameter sets to be created automatically.
# If no tuning grid is provided, a semi-random grid
# (via dials::grid_latin_hypercube()) is created with 10 candidate parameter combinations.

tictoc::tic()
best_hyperparameters <- forcedlabor::ml_hyperpar(train_pred_proba)
tictoc::toc()
stats_plot <- best_hyperparameters$auc_results
# saveRDS(object = stats_plot, file = "./outputs/stats_hyper_plot.rds")
# stats_plot <- readRDS(file = 'outputs/stats_hyper_plot.rds')

stats_plot <- stats_plot |>
  dplyr::select(mtry, min_n, regularization.factor, mean_performance)
# wite in a csv for python
readr::write_csv(x = stats_plot, file = "./outputs/hyper_data.csv")

### Graphs showing best hyperpar combination
# Set Python to python3
reticulate::use_python("/usr/bin/python3", required = TRUE)
# generate graphs fixing the optimal points
reticulate::source_python("./scripts/hyper_plot.py")


# Graphs of mtry vs min_n for AUC for each value of regularization.factor
ggplot2::ggplot(data = stats_plot, ggplot2::aes(y = mtry, x = min_n, color = mean_performance)) +
  ggplot2::geom_contour_filled(ggplot2::aes(z = mean_performance), bins = 30, show.legend = FALSE, alpha = 0.5) +
  ggplot2::geom_contour(ggplot2::aes(z = mean_performance, colour = ..level..), bins = 30) +
  ggplot2::facet_wrap(~ regularization.factor) +
  viridis::scale_fill_viridis(begin = 0, end = 1, option = "B", discrete = TRUE) +
  viridis::scale_color_viridis(begin = 0, end = 1, option = "B")  +
  ggplot2::scale_y_continuous(limits = range(stats_plot$mtry), breaks = unique(stats_plot$mtry)) +
  ggplot2::scale_x_continuous(limits = range(stats_plot$min_n), breaks = unique(stats_plot$min_n)) +
  ggplot2::labs(colour='AUC') +
  ggplot2::theme(strip.text.x = ggplot2::element_text(size = 50)) +
  ggplot2::theme_bw()
# ggplot2::ggsave(filename = "./outputs/hyper_sensitivity_inferno1.jpg", width = 8, height = 8, dpi = 600)
