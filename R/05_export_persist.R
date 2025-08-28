suppressPackageStartupMessages({ library(DBI); library(dplyr); library(readr) })
source("R/utils_db.R")

persist_schema <- "scratch"  # set to NULL to skip
table_name <- "icu_visits_with_chains"

if (!is.null(persist_schema)) {
  df <- readr::read_csv("data/icu_chains_chainlevel.csv", show_col_types = FALSE)
  
  with_conn({
    conn <- connect_pg()
    DBI::dbExecute(conn, sprintf("CREATE SCHEMA IF NOT EXISTS %s;", persist_schema))
    DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s.%s CASCADE;", persist_schema, table_name))
    DBI::dbWriteTable(conn,
                      name = DBI::Id(schema = persist_schema, table = table_name),
                      value = df,
                      overwrite = TRUE
    )
    DBI::dbExecute(conn, sprintf(
      "CREATE INDEX IF NOT EXISTS %s_%s_src_idx ON %s.%s (src_name);",
      persist_schema, table_name, persist_schema, table_name
    ))
    DBI::dbExecute(conn, sprintf(
      "CREATE INDEX IF NOT EXISTS %s_%s_person_idx ON %s.%s (person_id);",
      persist_schema, table_name, persist_schema, table_name
    ))
    DBI::dbExecute(conn, sprintf(
      "CREATE INDEX IF NOT EXISTS %s_%s_flags_idx ON %s.%s (icu_level_care, has_procedure, has_drugs);",
      persist_schema, table_name, persist_schema, table_name
    ))
    message(sprintf("Persisted to %s.%s with indexes", persist_schema, table_name))
  })
}
