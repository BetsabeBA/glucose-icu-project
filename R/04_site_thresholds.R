suppressPackageStartupMessages({ library(dplyr); library(readr); library(ggplot2) })

if (!dir.exists("figs")) dir.create("figs", recursive = TRUE)

icu_chains <- readr::read_csv("data/icu_chains_chainlevel.csv", show_col_types = FALSE)

site_thresh <- icu_chains |>
  group_by(src_name) |>
  summarize(
    p50_comb = quantile(combined_min_events_per_hour, 0.50, na.rm = TRUE),
    p75_comb = quantile(combined_min_events_per_hour, 0.75, na.rm = TRUE),
    p90_comb = quantile(combined_min_events_per_hour, 0.90, na.rm = TRUE),
    n = n(), .groups = "drop"
  ) |>
  arrange(src_name)

readr::write_csv(site_thresh, "data/icu_site_thresholds.csv")
print(site_thresh)

ggplot(icu_chains |> mutate(src_name = factor(src_name)),
       aes(x = combined_min_events_per_hour)) +
  geom_histogram(bins = 60) +
  facet_wrap(~ src_name, scales = "free_y") +
  labs(title = "Combined events per hour by site",
       x = "distinct-minute events/hour", y = "count of chains")

ggsave("figs/combined_events_hist_by_site.png", width = 10, height = 6, dpi = 300)
