library(yaml)
library(purrr)
library(bigrquery)


`%||%` <- function(x, y) {
  if (!is.null(x)) x else y
}

safe_table_parts <- function(
    table,
    project = NULL,
    dataset = NULL,
    scratch = NULL,
    scratch_location = NULL) {
  config <- yaml::read_yaml("./scripts/config.yaml")

  # Look for a table-specific configuration
  table_config <- purrr::keep(config$safe_table_get$tables, ~ .x$name == table)

  if (length(table_config) > 0) {
    # Override defaults with table-specific settings if null
    table_config <- table_config[[1]]
    project <- project %||% table_config$project
    dataset <- dataset %||% table_config$dataset
    scratch <- scratch %||% table_config$scratch
    scratch_location <- scratch_location %||% table_config$scratch_location
  }

  # Override defaults with top level settings if null
  project <- project %||% config$safe_table_get$project
  dataset <- dataset %||% config$safe_table_get$dataset
  scratch <- scratch %||% config$safe_table_get$scratch
  scratch_location <-
    scratch_location %||% config$safe_table_get$scratch_location

  # Override with scratch specific settings settings if scratch flag set
  if (scratch) {
    table <- paste0(dataset, "_", table)
    dataset <- scratch_location
  }

  return(list(project = project, dataset = dataset, table = table))
}

safe_table_addr <- function(
    table,
    project = NULL,
    dataset = NULL,
    scratch = NULL,
    scratch_location = NULL) {
  parts <- safe_table_parts(
    table,
    project = project, dataset = dataset,
    scratch = scratch, scratch_location = scratch_location
  )
  addr <- paste(parts$project, parts$dataset, parts$table, sep = ".")
  return(addr)
}

safe_table_get <- function(
    table,
    project = NULL,
    dataset = NULL,
    scratch = NULL,
    scratch_location = NULL) {
  parts <- safe_table_parts(
    table,
    project = project, dataset = dataset,
    scratch = scratch, scratch_location = scratch_location
  )
  t <- bigrquery::bq_table(
    project = parts$project,
    dataset = parts$dataset,
    table = parts$table
  )
  return(t)
}
