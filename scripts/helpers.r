library(yaml)
library(purrr)
library(bigrquery)


`%||%` <- function(x, y) {
  if (!is.null(x)) x else y
}

config <- yaml::read_yaml("config.yaml")

safe_table_parts <- function(
    table,
    project = config$safe_table_get$project,
    dataset = config$safe_table_get$dataset,
    scratch = config$safe_table_get$scratch,
    scratch_location = config$safe_table_get$scratch_location) {
  # Look for a table-specific configuration
  table_config <- purrr::keep(config$safe_table_get$tables, ~ .x$name == table)

  if (length(table_config) > 0) {
    # Override defaults with table-specific settings if found
    table_config <- table_config[[1]]
    project <- table_config$project %||% project
    dataset <- table_config$dataset %||% dataset
    scratch <- table_config$scratch %||% scratch
    scratch_location <- table_config$scratch_location %||% scratch_location
  }

  dataset_name <- if (scratch) scratch_location else dataset
  table_name <- if (scratch) paste0(dataset, "_", table) else table

  return(list(project = project, dataset = dataset_name, table = table_name))
}

safe_table_addr <- function(
    table,
    project = config$safe_table_get$project,
    dataset = config$safe_table_get$dataset,
    scratch = config$safe_table_get$scratch,
    scratch_location = config$safe_table_get$scratch_location) {
  parts <- safe_table_parts(
    table,
    project = project, dataset = dataset,
    scratch = scratch, scratch_location = scratch_location
  )
  addr <- paste(parts$project, parts$dataset, parts$table, sep = ".", " ")
  return(addr)
}

safe_table_get <- function(
    table,
    project = config$safe_table_get$project,
    dataset = config$safe_table_get$dataset,
    scratch = config$safe_table_get$scratch,
    scratch_location = config$safe_table_get$scratch_location) {
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
