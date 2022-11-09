
training_df <- readr::read_rds(file = "./outputs/training.rds")
# we don't train with known_offenders, so it doesn't have level 1 in known_non_offender
levels(training_df$known_non_offender) <- c(levels(training_df$known_non_offender),1)
prediction_df <- readr::read_rds(file = "./outputs/prediction.rds")
# we only have known_offenders in the prediction dataset, so it doesn't have level 0 in known_non_offender
levels(prediction_df$known_non_offender) <- c(levels(prediction_df$known_non_offender),0)


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

## defining some parameter values ##
num_folds <- 5 # number of folds
down_sample_ratio <- 1 # downsampling ratio
# Set common seed to use anywhere that uses random numbers
num_common_seeds <- 15
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

# About grid:
# An integer denotes the number of candidate parameter sets to be created automatically.
# If no tuning grid is provided, a semi-random grid
# (via dials::grid_latin_hypercube()) is created with 10 candidate parameter combinations.


########### FOR ALL SEEDS ##########################################

####### Classification with dedpul ########################################
# alpha value will be printed

tictoc::tic()
classif_res <- forcedlabor::ml_classification(data = train_pred_proba,
                                              steps = 1000,
                                              plotting = FALSE, filepath = NULL,
                                              threshold = seq(0, .99, by = 0.01),
                                              eps = 0.01)
tictoc::toc()

# load(file = "./outputs/outputs_group.RData")
# perf_15_1$alpha <- alpha_15_1

classif_res$alpha
# alpha_15_1
# [1] 0.2892893

# Recall and specificity
perf_15_1 <- forcedlabor::ml_perf_metrics(data = classif_res$pred_conf)
# recall    specif
# 1 0.8888889 0.9811321

perf_15_1$alpha <- classif_res$alpha

############# FUNCTIONS FOR GROUPS ########################################

# classification for each group of seeds
metrics_group <- function(groups, train_pred_proba){

  purrr::map(groups, function(group_id){
    group_seeds <- group_id*101
    sub_train_pred_proba <- train_pred_proba |>
      dplyr::filter(common_seed %in% group_seeds == TRUE)
    tictoc::tic()
    classif_res <- forcedlabor::ml_classification(data = sub_train_pred_proba,
                                                  steps = 1000, plotting = FALSE,
                                                  filepath = NULL,
                                                  threshold = seq(0, .99, by = 0.01),
                                                  eps = 0.01, parallel_plan = "multicore",
                                                  free_cores = 1)
    tictoc::toc()
    alpha_group <- classif_res$alpha
    # Recall and specificity
    perf_group <- forcedlabor::ml_perf_metrics(data = classif_res$pred_conf)

    return(list(alpha = alpha_group, recall = perf_group$recall,
                specif = perf_group$specif))

  })

}

# extracting vector of metric (alpha, recall or specif)
extract_vector <- function(param, list_group){
  metric_vector <-
    as.numeric(unlist(lapply(list_group, function (x) x[c(param)])))
  return(metric_vector)
}

# computing standard deviation
std <- function(x, na.rm = FALSE) {
  if (na.rm) x <- na.omit(x)
  sqrt(var(x) / length(x))
}


# getting stats when comparing with global results
stats_group <- function(main_value, group_results ){
  # diff with whole composite set
  dif_metric <- group_results - rep(main_value, length(group_results))
  mean_dif_metric <- mean(dif_metric)
  std_dif_metric <- std(dif_metric)

  data.frame(min = min(dif_metric), avg_minus_std_metric = mean_dif_metric -
               std_dif_metric,
             mean = mean_dif_metric, avg_plus_std_metric = mean_dif_metric +
               std_dif_metric,
             max = max(dif_metric))
}


# make plots (and save object in case I need to redo the plot)
stats_plot <- function(param){

  df <- rbind.data.frame(stats_group(main_value = perf_15_1[[param]],
                                     group_results = get(paste0(param,"_group_1"))),
                         stats_group(main_value = perf_15_1[[param]],
                                     group_results = get(paste0(param,"_group_3"))),
                         stats_group(main_value = perf_15_1[[param]],
                                     group_results = get(paste0(param,"_group_5")))) |>
    dplyr::mutate(seeds = c(1, 3, 5))

  # return(df)

  saveRDS(object = df, file = paste0('outputs/stats_seeds_',param,'_plot.rds'))

  if (param == "alpha"){
    ytext <- bquote(Delta ~ " " ~ alpha^"*")
  }else{
    ytext <- bquote(Delta ~ " " ~ .(param))
  }

  df |>
    ggplot2::ggplot(ggplot2::aes(x = seeds, y = mean)) +
    ggplot2::geom_point() +
    ggplot2::geom_errorbar(ggplot2::aes(ymin = min, ymax = max), width = 0.2,
                           linetype = 2) +
    ggplot2::geom_errorbar(ggplot2::aes(ymin = avg_minus_std_metric,
                                        ymax = avg_plus_std_metric), width = 0.3) +
    ggplot2::scale_x_continuous(breaks = c(1, 3, 5))+
    ggplot2::ylab(ytext) +
    ggplot2::theme_bw()
  ggplot2::ggsave(file = paste0("outputs/seed_sensitivity_",param,".png"))

}


## metrics for 3 groups of 5

groups <- list(1:5, 6:10, 11:15)
metrics_group_5 <- metrics_group(groups, train_pred_proba)
# saveRDS(object = metrics_group_5, file = paste0('metrics_group_5.rds'))
# metrics_group_5 <- readRDS('./outputs/metrics_group_5.rds')
alpha_group_5 <- extract_vector(param = "alpha", list_group = metrics_group_5)
recall_group_5 <- extract_vector(param = "recall", list_group = metrics_group_5)
specif_group_5 <- extract_vector(param = "specif", list_group = metrics_group_5)

## metrics for 5 groups of 3

groups <- list(1:3, 4:6, 7:9, 10:12, 13:15)
metrics_group_3 <- metrics_group(groups, train_pred_proba)
# saveRDS(object = metrics_group_3, file = paste0('metrics_group_3.rds'))
# metrics_group_3 <- readRDS('./outputs/metrics_group_3.rds')
alpha_group_3 <- extract_vector(param = "alpha", list_group = metrics_group_3)
recall_group_3 <- extract_vector(param = "recall", list_group = metrics_group_3)
specif_group_3 <- extract_vector(param = "specif", list_group = metrics_group_3)


# metrics of 15 groups of 1
groups <- as.list(1:15)
metrics_group_1 <- metrics_group(groups, train_pred_proba)
# saveRDS(object = metrics_group_1, file = paste0('metrics_group_1.rds'))
# metrics_group_1 <- readRDS('./outputs/metrics_group_1.rds')
alpha_group_1 <- extract_vector(param = "alpha", list_group = metrics_group_1)
recall_group_1 <- extract_vector(param = "recall", list_group = metrics_group_1)
specif_group_1 <- extract_vector(param = "specif", list_group = metrics_group_1)

#### using stats_group function

stats_plot("alpha")
stats_plot("recall")
stats_plot("specif")

