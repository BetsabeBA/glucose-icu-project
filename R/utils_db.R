suppressPackageStartupMessages({
  library(DBI); library(RPostgres); library(glue)
})

connect_pg <- function(
  host = Sys.getenv("PGHOST"),
  dbname = Sys.getenv("PGDATABASE"),
  user = Sys.getenv("PGUSER"),
  password = Sys.getenv("PGPASSWORD"),
  port = as.integer(Sys.getenv("PGPORT", "5432"))
) {
  dbConnect(Postgres(), host = host, dbname = dbname, user = user, password = password, port = port)
}

with_conn <- function(code) {
  conn <- connect_pg()
  on.exit(try(DBI::dbDisconnect(conn), silent = TRUE))
  force(code)
}

read_sql <- function(path) paste(readLines(path), collapse = "\n")
