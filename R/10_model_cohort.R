# Enrichment + survival-ready cohort (reads chain CSV; queries DB for comorbidities, insulin, glucose)
suppressPackageStartupMessages({
  library(DBI); library(RPostgres); library(dplyr); library(tidyr); library(lubridate)
  library(stringr); library(glue); library(bit64); library(readr)
})
source("R/utils_db.R")
source("R/utils_icu.R")

SCHEMA <- Sys.getenv("OMOP_SCHEMA", "omopcdmv2")

icu_chains_chainlevel <- readr::read_csv("data/icu_chains_chainlevel.csv", show_col_types = FALSE) |>
  mutate(
    chain_id    = as.integer(stringr::str_match(global_chain_id, ".*-(\\d+)$")[, 2]),
    src_name    = as.integer(src_name),
    chain_start = as.POSIXct(chain_start, tz = "UTC"),
    chain_end   = as.POSIXct(chain_end,   tz = "UTC")
  ) |>
  to_i64("person_id")

chains_for_db <- icu_chains_chainlevel |>
  transmute(person_id, src_name, chain_id, chain_start, chain_end)

with_conn({
  conn <- connect_pg()
  dbExecute(conn, "DROP TABLE IF EXISTS tmp_chain_windows")
  dbExecute(conn, "
    CREATE TEMP TABLE tmp_chain_windows (
      person_id BIGINT,
      src_name  INTEGER,
      chain_id  INTEGER,
      chain_start TIMESTAMP,
      chain_end   TIMESTAMP
    )")
  dbWriteTable(conn, name = DBI::SQL("tmp_chain_windows"), value = chains_for_db, append = TRUE, row.names = FALSE)

  insulin_ids <- c(
    1596977,19078555,1550023,19058398,1502905,1567198,19078557,19078559,
    46221581,1361675,19012678,35749499,46234049,19012266,1361676,19090249,
    1590165,19078554,1586346,19012507,19012679,19078556,40166274,19078552
  )
  sql_insulin <- glue("
    SELECT de.person_id, tcw.src_name, tcw.chain_id, COUNT(*)::int AS insulin_count
    FROM {`SCHEMA`}.drug_exposure de
    JOIN tmp_chain_windows tcw
      ON de.person_id = tcw.person_id
     AND de.drug_exposure_start_datetime >= tcw.chain_start
     AND de.drug_exposure_start_datetime <  tcw.chain_start + INTERVAL '24 hours'
    WHERE de.drug_concept_id IN ({paste(insulin_ids, collapse = ',')})
    GROUP BY de.person_id, tcw.src_name, tcw.chain_id
  ")
  insulin_counts <- dbGetQuery(conn, sql_insulin) |> to_i64("person_id")

  glu_ids <- c(3004501,3000483,3031266,3034962,44816672,3044242,82947,
               4144235,3014053,3011424,3033408,3004077,2212359,3037110,4149519)
  sql_glu <- glue("
    SELECT m.person_id, tcw.src_name, tcw.chain_id, m.measurement_datetime, m.value_as_number,
           tcw.chain_start, tcw.chain_end
    FROM {`SCHEMA`}.measurement m
    JOIN tmp_chain_windows tcw
      ON m.person_id = tcw.person_id
     AND m.measurement_datetime >= tcw.chain_start
     AND m.measurement_datetime <  LEAST(tcw.chain_end, tcw.chain_start + INTERVAL '24 hours')
    WHERE m.measurement_concept_id IN ({paste(glu_ids, collapse = ',')})
      AND m.measurement_datetime IS NOT NULL
  ")
  measurement_subset <- dbGetQuery(conn, sql_glu) |>
    mutate(
      person_id = bit64::as.integer64(person_id),
      measurement_datetime = as.POSIXct(measurement_datetime, tz = "UTC"),
      chain_start = as.POSIXct(chain_start, tz = "UTC"),
      chain_end   = as.POSIXct(chain_end,   tz = "UTC")
    )

  twa_glucose <- measurement_subset |>
    arrange(person_id, src_name, chain_id, measurement_datetime) |>
    group_by(person_id, src_name, chain_id) |>
    mutate(
      window_end = pmin(chain_end, chain_start + hours(24)),
      next_time  = lead(measurement_datetime),
      dt = ifelse(is.na(next_time),
                  as.numeric(difftime(window_end, measurement_datetime, units = "hours")),
                  as.numeric(difftime(next_time,   measurement_datetime, units = "hours")))
    ) |>
    filter(dt > 0, !is.na(value_as_number)) |>
    summarise(
      twa_glucose = ifelse(sum(dt, na.rm = TRUE) > 0,
                           sum(value_as_number * dt, na.rm = TRUE) / sum(dt, na.rm = TRUE),
                           NA_real_),
      first_measurement = min(measurement_datetime, na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(
      glucose_category_twa = dplyr::case_when(
        !is.na(twa_glucose) & twa_glucose <  70  ~ "Hypoglycemic",
        !is.na(twa_glucose) & twa_glucose <= 180 ~ "Normal",
        !is.na(twa_glucose) & twa_glucose >  180 ~ "Hyperglycemic",
        TRUE ~ NA_character_
      )
    ) |> to_i64("person_id")

  demographics <- dbGetQuery(conn, glue("
    SELECT person_id, year_of_birth, gender_concept_id
    FROM {`SCHEMA`}.person
  ")) |>
    to_i64("person_id") |>
    mutate(
      age = 2025 - as.integer(year_of_birth),
      sex = case_when(gender_concept_id == 8507 ~ 'Male',
                      gender_concept_id == 8532 ~ 'Female',
                      TRUE ~ NA_character_)
    ) |>
    select(person_id, age, sex)

  death_df <- dbGetQuery(conn, glue("SELECT person_id, death_date FROM {`SCHEMA`}.death")) |>
    to_i64("person_id")

  surv_index <- icu_chains_chainlevel |>
    select(person_id, src_name, chain_id, chain_start) |>
    left_join(twa_glucose |> select(person_id, src_name, chain_id, first_measurement),
              by = c("person_id","src_name","chain_id")) |>
    mutate(index_time = coalesce(first_measurement, chain_start)) |>
    left_join(death_df, by = "person_id") |>
    mutate(
      death_or_censor = pmin(as.POSIXct(death_date), index_time + days(28), na.rm = TRUE),
      survival_time   = as.numeric(difftime(death_or_censor, index_time, units = "days")),
      survival_time   = ifelse(is.na(survival_time) | survival_time < 0, 28, survival_time),
      status          = ifelse(!is.na(death_date) & death_date <= index_time + days(28), 1L, 0L)
    ) |>
    select(person_id, src_name, chain_id, survival_time, status)

  comorbidity_conditions <- list(
    diabetes=c(201820,4193704,201826),
    hypertension=c(316866,320128,4322024,192680,35207668,4339214,319826,4013643),
    heart_failure=c(315295,40480603,44782733,443587,40481042,44782719,4242669,319034,
                    439846,4233424,314378,44782718,35207793,45533456,37309625,35207673,
                    4014159,35207670,35207669,45576878,1326606,1569178,1326608,45586587),
    cancer=c(439847,443392),
    copd=c(255573,4110056,35208025,35208024),
    sepsis=c(132797,40493038,40491960,40487064,37394658,35205553,45577803,46269946,
             40487059,40491961,40489908,40489910,40486631,40493415,40489912,40489909,
             40487616,40489907,37312594,45548977,36716312,45532833,45552179,4102318,
             37163131,45595586),
    malnutrition=c(4233565,4098458,4156515,4096196,35206968,35206967,4101278,436078,437832),
    vte=c(440417,40481089,4108356,45772786,4327889,44782732,4110190,40479606,43530605,
          4213731,45572084,45768439,198446,4170620,4337373,432656,4258295,4006294,
          193512,40480461,1553749,4274969,45533441,45601094,35207867,45538453,45538456,
          375557,201730),
    aki=c(197320),
    endstage_renal_failure=c(4030520),
    liver_cirrhosis=c(4064161),
    ischemic_stroke=c(4310996)
  )

  comorbidity_query <- paste(
    lapply(names(comorbidity_conditions), function(nm) {
      ids <- paste(comorbidity_conditions[[nm]], collapse = ",")
      glue("
        SELECT co.person_id, '{nm}' AS comorbidity
        FROM {`SCHEMA`}.condition_occurrence co
        JOIN {`SCHEMA`}.concept_ancestor ca
          ON co.condition_concept_id = ca.descendant_concept_id
       WHERE ca.ancestor_concept_id IN ({ids})
      ")
    }),
    collapse = "\nUNION ALL\n"
  )

  full_comorbidity_query <- glue("
    WITH conditions AS (
      {comorbidity_query}
    )
    SELECT person_id,
           MAX(CASE WHEN comorbidity = 'diabetes' THEN 1 ELSE 0 END) AS diabetes,
           MAX(CASE WHEN comorbidity = 'hypertension' THEN 1 ELSE 0 END) AS hypertension,
           MAX(CASE WHEN comorbidity = 'heart_failure' THEN 1 ELSE 0 END) AS heart_failure,
           MAX(CASE WHEN comorbidity = 'copd' THEN 1 ELSE 0 END) AS copd,
           MAX(CASE WHEN comorbidity = 'cancer' THEN 1 ELSE 0 END) AS cancer,
           MAX(CASE WHEN comorbidity = 'sepsis' THEN 1 ELSE 0 END) AS sepsis,
           MAX(CASE WHEN comorbidity = 'malnutrition' THEN 1 ELSE 0 END) AS malnutrition,
           MAX(CASE WHEN comorbidity = 'vte' THEN 1 ELSE 0 END) AS vte,
           MAX(CASE WHEN comorbidity = 'aki' THEN 1 ELSE 0 END) AS aki,
           MAX(CASE WHEN comorbidity = 'endstage_renal_failure' THEN 1 ELSE 0 END) AS endstage_renal_failure,
           MAX(CASE WHEN comorbidity = 'liver_cirrhosis' THEN 1 ELSE 0 END) AS liver_cirrhosis,
           MAX(CASE WHEN comorbidity = 'ischemic_stroke' THEN 1 ELSE 0 END) AS ischemic_stroke
    FROM conditions
    GROUP BY person_id
  ")
  comorbidity_df <- dbGetQuery(conn, full_comorbidity_query) |> to_i64("person_id")

  cohort_merged <- icu_chains_chainlevel |>
    left_join(insulin_counts, by = c("person_id","src_name","chain_id")) |>
    left_join(twa_glucose,    by = c("person_id","src_name","chain_id")) |>
    left_join(surv_index,     by = c("person_id","src_name","chain_id")) |>
    left_join(demographics,   by = "person_id") |>
    left_join(comorbidity_df, by = "person_id") |>
    mutate(
      insulin_count = tidyr::replace_na(insulin_count, 0L),
      across(c(diabetes, hypertension, heart_failure, copd, cancer,
               sepsis, malnutrition, vte, aki,
               endstage_renal_failure, liver_cirrhosis, ischemic_stroke),
             ~ tidyr::replace_na(., 0L)),
      insulin_adm_cat_24h = case_when(
        insulin_count == 0  ~ "0",
        insulin_count <= 2  ~ "1-2",
        insulin_count <= 5  ~ "3-5",
        insulin_count <= 10 ~ "6-10",
        insulin_count > 10  ~ ">10",
        TRUE ~ NA_character_
      ),
      insulin_adm_cat_24h = factor(insulin_adm_cat_24h, levels = c("0","1-2","3-5","6-10",">10")),
      insulin_bin = factor(if_else(insulin_count == 0, "0", "1"), levels = c("0","1")),
      comorbidity_score = diabetes + hypertension + heart_failure + copd + cancer +
        sepsis + malnutrition + vte + aki +
        endstage_renal_failure + liver_cirrhosis + ischemic_stroke,
      comorbidity_group = cut(comorbidity_score, breaks = c(-Inf,0,1,2,3,Inf),
                              labels = c("0","1","2","3","4+"), right = TRUE),
      glucose_category_twa = factor(glucose_category_twa,
                                    levels = c("Normal","Hypoglycemic","Hyperglycemic")),
      sex = factor(sex, levels = c("Male","Female"))
    ) |>
    filter(!is.na(age) & age >= 18)

  cohort_merged_icu <- cohort_merged |>
    filter(icu_level_care == TRUE) |>
    mutate(
      glucose_qc_flag = case_when(
        is.na(twa_glucose) ~ "Missing",
        twa_glucose < 30   ~ "Low implausible",
        twa_glucose > 1000 ~ "High implausible",
        TRUE ~ "Valid"
      ),
      glucose_qc_flag = factor(glucose_qc_flag,
                               levels = c("Valid","Low implausible","High implausible","Missing"))
    ) |>
    filter(glucose_qc_flag == "Valid")

  if (!dir.exists("data")) dir.create("data", recursive = TRUE)
  write_csv(cohort_merged_icu, "data/cohort_merged_icu.csv")

  cat("Merged cohort dims (all chains):", paste(dim(cohort_merged), collapse=" x "), "\n")
  cat("Merged ICU cohort dims:", paste(dim(cohort_merged_icu), collapse=" x "), "\n")
  print(cohort_merged_icu |>
          count(insulin_adm_cat_24h, name = "chains") |>
          arrange(insulin_adm_cat_24h))
})
