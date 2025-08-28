source("R/utils_db.R")
with_conn({
  conn <- connect_pg()
  invisible(dbExecute(conn, "DROP TABLE IF EXISTS my_icu_procedure_concepts;"))
  invisible(dbExecute(conn, "DROP TABLE IF EXISTS my_icu_vaso_descendants;"))
  dbExecute(conn, read_sql("sql/concept_sets_procedures.sql"))
  dbExecute(conn, read_sql("sql/concept_sets_vaso_desc.sql"))
  cat("Concept sets created (TEMP tables)\n")
})
