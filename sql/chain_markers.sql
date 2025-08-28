WITH visits AS (
  SELECT person_id, visit_occurrence_id, visit_start_datetime, visit_end_datetime,
         CAST(src_name AS INTEGER) AS src_name, visit_concept_id
  FROM omopcdmv2.visit_occurrence
  WHERE visit_concept_id = 9201
    AND visit_start_datetime IS NOT NULL
    AND visit_end_datetime   IS NOT NULL
    AND CAST(src_name AS INTEGER) BETWEEN 1 AND 13
),
ordered_visits AS (
  SELECT *,
         LAG(visit_end_datetime) OVER (
           PARTITION BY person_id, src_name
           ORDER BY visit_start_datetime
         ) AS prev_end
  FROM visits
),
flag_chains AS (
  SELECT *,
         CASE
           WHEN prev_end IS NULL THEN 1
           WHEN visit_start_datetime <= prev_end THEN 0
           WHEN visit_start_datetime - prev_end <= INTERVAL 'TOKEN_GAP_HOURS hours' THEN 0
           ELSE 1
         END AS chain_break
  FROM ordered_visits
),
grouped_chains AS (
  SELECT *,
         SUM(chain_break) OVER (
           PARTITION BY person_id, src_name
           ORDER BY visit_start_datetime
         ) AS chain_id
  FROM flag_chains
),
chained_visits AS (
  SELECT person_id, visit_occurrence_id, src_name, visit_start_datetime, visit_end_datetime,
         CONCAT(person_id, '-', src_name, '-', chain_id) AS global_chain_id
  FROM grouped_chains
),
chain_windows AS (
  SELECT
    global_chain_id,
    MIN(person_id) AS person_id,
    MIN(src_name)  AS src_name,
    MIN(visit_start_datetime) AS chain_start,
    MAX(visit_end_datetime)   AS chain_end
  FROM chained_visits
  GROUP BY global_chain_id
),
has_procedure AS (
  SELECT DISTINCT cw.global_chain_id
  FROM chain_windows cw
  JOIN omopcdmv2.procedure_occurrence po
    ON po.person_id = cw.person_id
   AND COALESCE(po.procedure_datetime, po.procedure_date::timestamp) >= cw.chain_start
   AND COALESCE(po.procedure_datetime, po.procedure_date::timestamp) <  cw.chain_end
  JOIN my_icu_procedure_concepts c
    ON po.procedure_concept_id = c.concept_id
),
iv_routes AS (
  SELECT concept_id
  FROM omopcdmv2.concept
  WHERE domain_id='Route' AND lower(concept_name) LIKE '%%intraven%%'
),
has_drugs AS (
  SELECT DISTINCT cw.global_chain_id
  FROM chain_windows cw
  JOIN omopcdmv2.drug_exposure de
    ON de.person_id = cw.person_id
   AND COALESCE(de.drug_exposure_start_datetime, de.drug_exposure_start_date::timestamp) <  cw.chain_end
   AND COALESCE(de.drug_exposure_end_datetime,   de.drug_exposure_end_date::timestamp,
                de.drug_exposure_start_datetime,
                de.drug_exposure_start_date::timestamp) >= cw.chain_start
  JOIN my_icu_vaso_descendants v
    ON de.drug_concept_id = v.concept_id
  JOIN iv_routes r
    ON de.route_concept_id = r.concept_id
),
measurement_count AS (
  SELECT
    cv.global_chain_id,
    COUNT(*) AS num_measurements,
    COUNT(DISTINCT m.measurement_concept_id) AS num_measurement_types,
    COUNT(DISTINCT date_trunc('minute', m.measurement_datetime)) AS distinct_minute_events
  FROM chained_visits cv
  JOIN chain_windows cw USING (global_chain_id)
  JOIN omopcdmv2.measurement m
    ON m.person_id = cv.person_id
   AND m.measurement_datetime >= cw.chain_start
   AND m.measurement_datetime <  cw.chain_end
  GROUP BY cv.global_chain_id
),
observation_count AS (
  SELECT
    cw.global_chain_id,
    COUNT(*) AS num_observations,
    COUNT(DISTINCT date_trunc('minute', o.observation_datetime)) AS obs_distinct_minute_events
  FROM chain_windows cw
  JOIN omopcdmv2.observation o
    ON o.person_id = cw.person_id
   AND o.observation_datetime >= cw.chain_start
   AND o.observation_datetime <  cw.chain_end
  GROUP BY cw.global_chain_id
)
SELECT
  cw.person_id,
  cw.src_name,
  cw.global_chain_id,
  cw.chain_start,
  cw.chain_end,
  EXTRACT(EPOCH FROM (cw.chain_end - cw.chain_start)) / 3600.0 AS hospital_los_hours,
  (hp.global_chain_id IS NOT NULL) AS has_procedure,
  (hd.global_chain_id IS NOT NULL) AS has_drugs,
  COALESCE(mc.num_measurements, 0)               AS num_measurements,
  COALESCE(mc.num_measurement_types, 0)          AS num_measurement_types,
  COALESCE(mc.distinct_minute_events, 0)         AS distinct_minute_events,
  COALESCE(oc.num_observations, 0)               AS num_observations,
  COALESCE(oc.obs_distinct_minute_events, 0)     AS obs_distinct_minute_events,
  COALESCE(
    mc.num_measurements::float
    / NULLIF(EXTRACT(EPOCH FROM (cw.chain_end - cw.chain_start)) / 3600.0, 0.0),
    0.0
  ) AS meas_per_hour,
  COALESCE(
    mc.distinct_minute_events::float
    / NULLIF(EXTRACT(EPOCH FROM (cw.chain_end - cw.chain_start)) / 3600.0, 0.0),
    0.0
  ) AS meas_min_events_per_hour,
  COALESCE(
    oc.obs_distinct_minute_events::float
    / NULLIF(EXTRACT(EPOCH FROM (cw.chain_end - cw.chain_start)) / 3600.0, 0.0),
    0.0
  ) AS obs_min_events_per_hour,
  GREATEST(
    COALESCE(
      mc.distinct_minute_events::float
      / NULLIF(EXTRACT(EPOCH FROM (cw.chain_end - cw.chain_start)) / 3600.0, 0.0),
      0.0
    ),
    COALESCE(
      oc.obs_distinct_minute_events::float
      / NULLIF(EXTRACT(EPOCH FROM (cw.chain_end - cw.chain_start)) / 3600.0, 0.0),
      0.0
    )
  ) AS combined_min_events_per_hour,
  (hp.global_chain_id IS NOT NULL) AS icu_by_procedure,
  (hd.global_chain_id IS NOT NULL) AS icu_by_drug,
  (
    GREATEST(
      COALESCE(
        mc.distinct_minute_events::float
        / NULLIF(EXTRACT(EPOCH FROM (cw.chain_end - cw.chain_start)) / 3600.0, 0.0),
        0.0
      ),
      COALESCE(
        oc.obs_distinct_minute_events::float
        / NULLIF(EXTRACT(EPOCH FROM (cw.chain_end - cw.chain_start)) / 3600.0, 0.0),
        0.0
      )
    ) >= TOKEN_RATE_MIN AND COALESCE(mc.num_measurement_types, 0) >= TOKEN_MIN_TYPES
  ) AS icu_by_monitoring,
  CASE
    WHEN (hp.global_chain_id IS NOT NULL)
      OR (hd.global_chain_id IS NOT NULL)
      OR (
           GREATEST(
             COALESCE(
               mc.distinct_minute_events::float
               / NULLIF(EXTRACT(EPOCH FROM (cw.chain_end - cw.chain_start)) / 3600.0, 0.0),
               0.0
             ),
             COALESCE(
               oc.obs_distinct_minute_events::float
               / NULLIF(EXTRACT(EPOCH FROM (cw.chain_end - cw.chain_start)) / 3600.0, 0.0),
               0.0
             )
           ) >= TOKEN_RATE_MIN
           AND COALESCE(mc.num_measurement_types, 0) >= TOKEN_MIN_TYPES
         )
      OR (
           COALESCE(
             mc.num_measurements::float
             / NULLIF(EXTRACT(EPOCH FROM (cw.chain_end - cw.chain_start)) / 3600.0, 0.0),
             0.0
           ) >= TOKEN_RATE_RAW
           AND COALESCE(mc.num_measurement_types, 0) >= TOKEN_MIN_TYPES
         )
    THEN TRUE ELSE FALSE
  END AS icu_level_care
INTO TEMPORARY icu_visits_with_chains_temp
FROM chain_windows cw
LEFT JOIN has_procedure   hp ON cw.global_chain_id = hp.global_chain_id
LEFT JOIN has_drugs       hd ON cw.global_chain_id = hd.global_chain_id
LEFT JOIN measurement_count mc ON cw.global_chain_id = mc.global_chain_id
LEFT JOIN observation_count oc ON cw.global_chain_id = oc.global_chain_id;
