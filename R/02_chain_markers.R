suppressPackageStartupMessages({ library(dplyr); library(DBI); library(readr) })
source("R/utils_db.R")

run_chain_markers <- function(gap_hours = 6,
                              rate_min_threshold = 1.0,
                              rate_raw_threshold = 2.0,
                              min_types_threshold = 6) {
  
  if (!dir.exists("data")) dir.create("data", recursive = TRUE)
  
  sql <- read_sql("sql/chain_markers.sql")
  # Replace tokens sequentially (no pipes / placeholders)
  sql <- gsub("TOKEN_GAP_HOURS", as.character(gap_hours),        sql, fixed = TRUE)
  sql <- gsub("TOKEN_RATE_MIN",  as.character(rate_min_threshold),sql, fixed = TRUE)
  sql <- gsub("TOKEN_RATE_RAW",  as.character(rate_raw_threshold),sql, fixed = TRUE)
  sql <- gsub("TOKEN_MIN_TYPES", as.character(min_types_threshold),sql, fixed = TRUE)
  
  message("Step 01: (re)create concept temp tables for this connection...")
  with_conn({
    conn <- connect_pg()
    
    invisible(dbExecute(conn, "DROP TABLE IF EXISTS my_icu_procedure_concepts;"))
    invisible(dbExecute(conn, "DROP TABLE IF EXISTS my_icu_vaso_descendants;"))
    dbExecute(conn, read_sql("sql/concept_sets_procedures.sql"))
    dbExecute(conn, read_sql("sql/concept_sets_vaso_desc.sql"))
    
    message("Step 02: create temp ICU chain table from SQL...")
    n <- tryCatch({
      dbExecute(conn, sql)
    }, error = function(e) {
      cat("\n--- SQL execution failed ---\n", conditionMessage(e), "\n", sep = "")
      cat("\nFirst 600 chars of rendered SQL:\n", substr(sql, 1, 600), "...\n", sep = "")
      stop("Aborting: chain_markers SQL failed. Check tokens and schema names.")
    })
    message(sprintf("Temp chain table created (rows affected: %s)", format(n, big.mark=",")))
    
    message("Step 03: fetching results to R and writing CSV...")
    df <- dbGetQuery(conn, "SELECT * FROM icu_visits_with_chains_temp")
    readr::write_csv(df, "data/icu_chains_chainlevel.csv")
    message("Wrote data/icu_chains_chainlevel.csv")
    invisible(df)
  })
}

# Run directly if you source the file
if (sys.nframe() == 0) run_chain_markers()
