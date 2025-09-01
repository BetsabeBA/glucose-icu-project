# ICU-Level Care Cohort (OMOP CDM)

Derives ICU-level care chains from OMOP CDM using (1) ICU procedures, (2) IV vasopressors, and (3) monitoring intensity, with site-adjusted thresholds and a survival-ready cohort.

---

## Prereqs
- R ≥ 4.2
- Postgres access to your OMOP schema  
  (read on person, visit, procedure, drug_exposure, measurement, observation, death; read on concept tables)
- **Azure Postgres note:** connections use `sslmode=require`. Some tenants require `PGUSER` like `username@servername`.

---

## Quick start

1) **Set credentials** (project root):
   - Copy `.Renviron.example` → `.Renviron`, then edit:
     ```
     PGHOST=XXX.com
     PGPORT=5432
     PGDATABASE=aimahead
     PGUSER=aimahead            # or aimahead@psql-aimahead-e2 if Azure requires it
     PGPASSWORD=********
     OMOP_SCHEMA=omopcdmv2
     ```
   - Restart R or run `readRenviron(".Renviron")`.

2) **Install deps + lock**
```r
install.packages("renv")
renv::init()
renv::restore()  # on a new machine

# ICU-Level Care Cohort (OMOP CDM)

Derives ICU-level care chains from OMOP CDM using (1) ICU procedures, (2) IV vasopressors, and (3) monitoring intensity, with site-adjusted thresholds and a survival-ready cohort.

---

## Prereqs
- R ≥ 4.2
- Postgres access to your OMOP schema  
  (read on person, visit, procedure, drug_exposure, measurement, observation, death; read on concept tables)
- **Azure Postgres note:** connections use `sslmode=require`. Some tenants require `PGUSER` like `username@servername`.

---

## Quick start

1) **Set credentials** (project root):
   - Copy `.Renviron.example` → `.Renviron`, then edit:
     ```
     PGHOST=psql-aimahead-e2.postgres.database.azure.com
     PGPORT=5432
     PGDATABASE=aimahead
     PGUSER=aimahead            # or aimahead@psql-aimahead-e2 if Azure requires it
     PGPASSWORD=********
     OMOP_SCHEMA=omopcdmv2
     ```
   - Restart R or run `readRenviron(".Renviron")`.

2) **Install deps + lock**
```r
install.packages("renv")
renv::init()
renv::restore()  # on a new machine

# Run
source("R/99_main.R")


## Run with {targets} (optional)
```r
install.packages(c("targets","tarchetypes"))
targets::tar_make()
# Visualize pipeline (optional):
# targets::tar_visnetwork()



---

### 3) Outputs
```md
## Outputs
- `data/icu_chains_chainlevel.csv` — one row per chain with ICU markers
- `data/icu_by_site_summary.csv` — site-level counts and percentages
- `data/icu_site_thresholds.csv` — p50/p75/p90 monitoring-rate thresholds by site
- `figs/combined_events_hist_by_site.png` — histogram of combined events/hour by site
- `data/cohort_merged_icu.csv` — survival-ready ICU cohort (optional downstream)
> `data/` and `figs/` are gitignored by default; commit only de-identified outputs.

## Configuration / Tuneables
Edit defaults in `R/02_chain_markers.R` (or override in your call to `run_chain_markers()`):
- `gap_hours = 6` — visit chaining gap (hours)
- `min_types_threshold = 6` — required distinct measurement concepts
- `rate_min_threshold = 1.0` — preferred events/hour threshold
- `rate_raw_threshold = 2.0` — fallback rows/hour threshold
The SQL in `sql/chain_markers.sql` uses placeholders `TOKEN_*` that R replaces via `gsub()`.

## Common pitfalls
**Temp tables are session-scoped.**  
`02_chain_markers` creates a TEMP table and then writes a CSV; downstream steps read the CSV.  
Error `relation "icu_visits_with_chains_temp" does not exist` → run `R/02_chain_markers.R` first.

**Local socket error**  
`could not connect ... /var/run/postgresql/.s.PGSQL.5432` → your `.Renviron` didn’t load.  
Run:
```r
readRenviron(".Renviron")
Sys.getenv(c("PGHOST","PGUSER","PGPASSWORD"))


---

### 6) Troubleshooting checklist
```md
## Troubleshooting checklist
```r
# 1) Credentials loaded?
readRenviron(".Renviron")
Sys.getenv(c("PGHOST","PGPORT","PGDATABASE","PGUSER","OMOP_SCHEMA"))

# 2) DB reachable?
source("R/utils_db.R")
con <- connect_pg()
DBI::dbGetQuery(con, "SELECT COUNT(*) FROM omopcdmv2.person LIMIT 1;")
DBI::dbDisconnect(con)

# 3) After step 02, does the CSV exist?
file.exists("data/icu_chains_chainlevel.csv")

# 4) Inspect first rows/columns quickly
readr::read_csv("data/icu_chains_chainlevel.csv", n_max = 5, show_col_types = FALSE)

---

### 7) Project structure
```md
## Project structure
R/
  utils_db.R            # connection helpers (sslmode=require)
  01_concept_sets.R     # TEMP concept tables
  02_chain_markers.R    # renders SQL, writes chain CSV
  03_diagnostics.R      # summaries from CSV
  04_site_thresholds.R  # thresholds + histogram
  05_export_persist.R   # optional: persist to DB schema
  10_model_cohort.R     # optional: survival-ready cohort
  99_main.R             # classic driver
sql/
  concept_sets_procedures.sql
  concept_sets_vaso_desc.sql
  chain_markers.sql     # uses TOKEN_* placeholders
_targets.R              # optional {targets} pipeline



---

### 8) Persistent DB mode (optional)
```md
## Optional: Persistent DB mode
If you prefer a DB table instead of CSV handoff:
1. In `sql/chain_markers.sql`, replace  
   `INTO TEMPORARY icu_visits_with_chains_temp` with:

---

### 9) Security & PHI
```md
## Security & PHI
- Never commit `.Renviron` (already in `.gitignore`).
- Only commit de-identified/synthetic outputs from `data/` or `figs/`.
- Follow CHoRUS/AIM-AHEAD data governance for source DB access.


