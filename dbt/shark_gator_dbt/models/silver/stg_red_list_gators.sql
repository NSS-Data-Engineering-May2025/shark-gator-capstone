{{config(
    materialized='view',
    tags= ['staging', 'silver', 'red-list', 'gators','monthly'],
)}}

SELECT DISTINCT
    A.assessmentId AS ASSESSMENT_ID,
    A.internalTaxonId AS TAXON_ID,
    A.scientificName AS SCIENTIFIC_NAME,
    T.genusName AS GENUS_NAME,
    LOWER(T.speciesName) AS SPECIES_NAME,
    LOWER(C.name) AS COMMON_NAME,
    C.main AS MAIN_NAME,
    A.redlistCategory AS RED_LIST_CATEGORY,
    CAST(A.yearPublished AS INTEGER) AS YEAR_PUBLISHED,
    TO_TIMESTAMP(REPLACE(A.assessmentDate, 'UTC', '')) AS ASSESSMENT_DATE,
    CAST(A.criteriaVersion AS FLOAT) AS CRITERIA_VERSION,
    A.rationale AS RATIONALE,
    A.habitat AS HABITAT,
    A.threats AS THREATS,
    A.population AS POPULATION,
    A.populationTrend AS POPULATION_TREND,
    A.range AS GLOBAL_LOCATION_RANGE,
    A.useTrade AS USE_TRADE,
    TRIM(REPLACE(systems_flattened.value, '(=Inland waters)', '(Inland waters)')) AS ENVIRONMENT,
    A.conservationActions AS CONSERVATION_ACTIONS,
    TRIM(realm_flattened.value) AS GLOBAL_REALM,
    CAST(A.possiblyExtinct AS BOOLEAN) AS POSSIBLY_EXTINCT,
    CAST(A.possiblyExtinctInTheWild AS BOOLEAN) AS POSSIBLY_EXTINCT_IN_WILD
FROM {{ source('bronze_source', 'RAW_GATOR_RED_LIST_ASSESSMENTS') }} A
JOIN LATERAL FLATTEN(INPUT => SPLIT(A.realm, '|')) AS realm_flattened
JOIN LATERAL FLATTEN(INPUT => SPLIT(A.systems, '|')) AS systems_flattened
JOIN {{ source('bronze_source', 'RAW_GATOR_RED_LIST_COMMON_NAMES') }} C
ON A.internalTaxonId = C.internalTaxonId
JOIN {{ source('bronze_source', 'RAW_GATOR_RED_LIST_TAXONOMY') }} T
ON T.internalTaxonId = A.internalTaxonId
