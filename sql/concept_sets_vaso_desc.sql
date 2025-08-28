WITH drug_ingredients AS (
  SELECT concept_id, lower(concept_name) AS seed_name
  FROM omopcdmv2.concept
  WHERE domain_id='Drug' AND vocabulary_id='RxNorm' AND concept_class_id='Ingredient'
    AND lower(concept_name) IN (
      'norepinephrine','noradrenaline','epinephrine','adrenaline',
      'dopamine','phenylephrine','vasopressin','dobutamine'
    )
),
drug_desc AS (
  SELECT ca.descendant_concept_id AS concept_id
  FROM omopcdmv2.concept_ancestor ca
  JOIN drug_ingredients s ON ca.ancestor_concept_id = s.concept_id
  UNION
  SELECT concept_id FROM drug_ingredients
)
SELECT DISTINCT d.concept_id
INTO TEMP my_icu_vaso_descendants
FROM drug_desc d
JOIN omopcdmv2.concept c ON c.concept_id = d.concept_id
WHERE c.standard_concept = 'S'
  AND c.invalid_reason IS NULL
  AND c.domain_id = 'Drug'
  AND c.concept_class_id IN (
    'Ingredient','Clinical Drug','Branded Drug',
    'Clinical Dose Group','Branded Dose Group'
  );
