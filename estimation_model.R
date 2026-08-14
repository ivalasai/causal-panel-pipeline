#!/usr/bin/env Rscript
# ------------------------------------------------------------------------------
# Generic staggered event-study estimation template
#
# Fits a Sun & Abraham (2021) interaction-weighted event study using fixest,
# with unit and time fixed effects. Replace placeholder column names with your
# schema before running in production.
# ------------------------------------------------------------------------------

suppressPackageStartupMessages({
  if (!requireNamespace("fixest", quietly = TRUE)) {
    stop(
      "Package 'fixest' is required. Install with: install.packages('fixest')",
      call. = FALSE
    )
  }
})

library(fixest)

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

PANEL_PATH       <- "data/processed/panel_table.csv"
OUTPUT_DIR       <- "output"
OUTCOME_VAR      <- "metric_deviation"   # dependent variable
UNIT_VAR         <- "entity_id"          # panel unit identifier
TIME_VAR         <- "period_id"          # calendar index (e.g., year or YYYYMM)
COHORT_VAR       <- "cohort_id"          # first-treatment timing (gname)
PERIOD_REL_VAR   <- "event_time"         # relative period (t − g)
REF_PERIOD       <- -1L                  # omitted reference period for sunab()

# Optional absorbed fixed effects (edit formula as needed)
FE_TERMS <- c(TIME_VAR, "group_id")

# Clustering dimension for inference (rule / cohort / region)
CLUSTER_VAR <- "cohort_id"

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

ensure_output_dir <- function(path) {
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE)
  }
}

load_panel <- function(path) {
  if (!file.exists(path)) {
    stop(sprintf("Panel file not found: %s", path), call. = FALSE)
  }
  df <- read.csv(path, stringsAsFactors = FALSE)
  message(sprintf("Loaded panel: %s rows, %s columns", nrow(df), ncol(df)))
  df
}

validate_columns <- function(df, required) {
  missing <- setdiff(required, names(df))
  if (length(missing) > 0) {
    stop(
      sprintf("Panel is missing required columns: %s", paste(missing, collapse = ", ")),
      call. = FALSE
    )
  }
}

# ------------------------------------------------------------------------------
# Estimation
# ------------------------------------------------------------------------------

run_event_study <- function(df) {
  required <- c(OUTCOME_VAR, UNIT_VAR, TIME_VAR, COHORT_VAR, PERIOD_REL_VAR)
  validate_columns(df, required)

  # Restrict to treated and not-yet-treated units as appropriate for your design.
  # The template keeps all rows; add filters here if needed.
  estimation_df <- df

  # Build fixed-effect RHS: unit + optional additional absorbable terms
  fe_rhs <- paste(unique(c(UNIT_VAR, FE_TERMS)), collapse = " + ")

  formula_str <- sprintf(
    "%s ~ sunab(%s, %s, ref.p = %d) | %s",
    OUTCOME_VAR,
    COHORT_VAR,
    PERIOD_REL_VAR,
    REF_PERIOD,
    fe_rhs
  )

  message("Estimating model:")
  message("  ", formula_str)

  model <- feols(
    as.formula(formula_str),
    data    = estimation_df,
    cluster = as.formula(sprintf("~%s", CLUSTER_VAR))
  )

  model
}

summarize_dynamic_effects <- function(model) {
  # Aggregate post-treatment average effect (customize event window as needed)
  agg <- summary(model, agg = "att")
  print(agg)
  invisible(agg)
}

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

main <- function() {
  ensure_output_dir(OUTPUT_DIR)

  panel <- load_panel(PANEL_PATH)
  model <- run_event_study(panel)

  # Console summary
  print(summary(model))
  agg <- summarize_dynamic_effects(model)

  # Persist coefficient table
  coef_path <- file.path(OUTPUT_DIR, "event_study_coefficients.csv")
  coef_table <- as.data.frame(coeftable(model))
  write.csv(coef_table, coef_path, row.names = TRUE)
  message(sprintf("Coefficients written to %s", coef_path))

  # Persist model object for reproducibility
  model_path <- file.path(OUTPUT_DIR, "event_study_model.rds")
  saveRDS(model, model_path)
  message(sprintf("Model object saved to %s", model_path))

  invisible(list(model = model, aggregate = agg))
}

if (sys.nframe() == 0) {
  main()
}
