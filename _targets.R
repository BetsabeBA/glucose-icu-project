# {targets} pipeline orchestrating the ICU chain build.
suppressPackageStartupMessages({
  library(targets)
  library(tarchetypes)
})

tar_option_set(packages = c(
  "DBI","RPostgres","dplyr","readr","ggplot2","glue","bit64","lubridate","stringr"
))

# Source functions / scripts. Some targets call functions defined inside these scripts.
source("R/utils_db.R", local = TRUE)
source("R/utils_icu.R", local = TRUE)
source("R/02_chain_markers.R", local = TRUE)

list(
  tar_target(concept_sets, {
    source("R/01_concept_sets.R")
    "concept_sets_ok"
  }),
  tar_target(icu_chains_csv, {
    # run_chain_markers() defined in R/02_chain_markers.R
    run_chain_markers()
    "data/icu_chains_chainlevel.csv"
  }, format = "file", deps = concept_sets),
  tar_target(by_site_csv, {
    source("R/03_diagnostics.R")
    "data/icu_by_site_summary.csv"
  }, format = "file", deps = icu_chains_csv),
  tar_target(site_thresh_csv, {
    source("R/04_site_thresholds.R")
    "data/icu_site_thresholds.csv"
  }, format = "file", deps = icu_chains_csv),
  tar_target(hist_png, {
    "figs/combined_events_hist_by_site.png"
  }, format = "file", deps = site_thresh_csv),
  tar_target(cohort_csv, {
    # optional downstream modeling step
    if (file.exists("R/10_model_cohort.R")) {
      source("R/10_model_cohort.R")
      "data/cohort_merged_icu.csv"
    } else {
      NA_character_
    }
  }, format = "file", deps = icu_chains_csv)
)
