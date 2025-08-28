WITH proc_seeds AS (
  SELECT concept_id
  FROM omopcdmv2.concept
  WHERE domain_id = 'Procedure'
    AND vocabulary_id IN ('SNOMED','CPT4','HCPCS')
    AND (
      lower(concept_name) LIKE '%mechanical ventilation%' OR
      lower(concept_name) LIKE '%endotracheal intubation%' OR
      lower(concept_name) LIKE '%insertion of arterial line%' OR
      lower(concept_name) LIKE '%arterial catheter%' OR
      lower(concept_name) LIKE '%central venous catheter%' OR
      lower(concept_name) LIKE '%central line insertion%'
    )
),
proc_desc AS (
  SELECT ca.descendant_concept_id AS concept_id
  FROM omopcdmv2.concept_ancestor ca
  JOIN proc_seeds s ON ca.ancestor_concept_id = s.concept_id
  UNION
  SELECT concept_id FROM proc_seeds
)
SELECT DISTINCT d.concept_id
INTO TEMP my_icu_procedure_concepts
FROM proc_desc d
JOIN omopcdmv2.concept c ON c.concept_id = d.concept_id
WHERE c.standard_concept = 'S'
  AND c.invalid_reason IS NULL
  AND c.domain_id = 'Procedure';
