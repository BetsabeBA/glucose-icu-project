suppressPackageStartupMessages({ library(dplyr); library(readr) })

df <- readr::read_csv("data/icu_chains_chainlevel.csv", show_col_types = FALSE)

# "Peek" of ICU rows
peek <- df %>%
  filter(icu_level_care) %>%
  arrange(chain_start) %>%
  select(person_id, src_name, global_chain_id, chain_start, chain_end,
         hospital_los_hours, has_procedure, has_drugs,
         num_measurements, num_measurement_types,
         meas_per_hour, meas_min_events_per_hour, obs_min_events_per_hour,
         combined_min_events_per_hour,
         icu_by_procedure, icu_by_drug, icu_by_monitoring, icu_level_care) %>%
  slice_head(n = 10)

print(peek)

# Reason overlap
overlap <- df %>%
  summarise(
    by_proc   = sum(as.integer(icu_by_procedure), na.rm = TRUE),
    by_drug   = sum(as.integer(icu_by_drug), na.rm = TRUE),
    by_monitor= sum(as.integer(icu_by_monitoring), na.rm = TRUE),
    any_icu   = sum(as.integer(icu_level_care), na.rm = TRUE)
  )
print(overlap)

# By-site summary + percentages
by_site <- df %>%
  group_by(src_name) %>%
  summarise(
    chains    = n(),
    n_proc    = sum(as.integer(has_procedure), na.rm = TRUE),
    n_drug    = sum(as.integer(has_drugs), na.rm = TRUE),
    n_icu     = sum(as.integer(icu_level_care), na.rm = TRUE),
    n_monitor = sum(as.integer(icu_by_monitoring), na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    pct_proc    = round(100 * n_proc / chains, 1),
    pct_drug    = round(100 * n_drug / chains, 1),
    pct_icu     = round(100 * n_icu  / chains, 1),
    pct_monitor = round(100 * n_monitor / chains, 1)
  ) %>%
  arrange(src_name)

if (!dir.exists("data")) dir.create("data", recursive = TRUE)
readr::write_csv(by_site, "data/icu_by_site_summary.csv")
print(by_site)
