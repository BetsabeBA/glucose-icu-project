# One-command driver (assumes .Renviron is set)
if (!dir.exists("data")) dir.create("data", recursive = TRUE)
if (!dir.exists("figs")) dir.create("figs", recursive = TRUE)

source("R/01_concept_sets.R")
source("R/02_chain_markers.R")      # writes data/icu_chains_chainlevel.csv
source("R/03_diagnostics.R")
source("R/04_site_thresholds.R")
# Optional:
# source("R/05_export_persist.R")
# source("R/10_model_cohort.R")     # creates data/cohort_merged_icu.csv
