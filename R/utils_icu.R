suppressPackageStartupMessages({
  library(dplyr); library(readr); library(stringr); library(bit64)
})

to_i64 <- function(df, cols = "person_id") {
  df %>% mutate(across(all_of(cols), ~ bit64::as.integer64(.)))
}
