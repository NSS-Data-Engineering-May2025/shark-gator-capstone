{{config(
    materialized='table',
    tags= ['models', 'gold', 'gators', 'monthly'],
    schema='GOLD'
)}}

SELECT DISTINCT
    GA.ATTACK_ID,
    GA.SPECIES,
    GA.LATITUDE,
    GA.LONGITUDE,
    GA.PROVINCE_STATE,
    GA.COUNTRY,
    GA.FATAL_OUTCOME,
    GA.EXACT_ATTACK_DATE,
    GA.ATTACK_YEAR,
    RG.ASSESSMENT_ID,
    RG.TAXON_ID,
    RG.SCIENTIFIC_NAME,
    RG.SPECIES_NAME,
    RG.COMMON_NAME,
    RG.RED_LIST_CATEGORY,
    RG.YEAR_PUBLISHED AS RED_LIST_YEAR,
    RG.ASSESSMENT_DATE,
    RG.HABITAT,
    RG.THREATS,
    RG.POPULATION,
    RG.POPULATION_TREND,
    RG.GLOBAL_LOCATION_RANGE,
    RG.ENVIRONMENT,
    RG.GLOBAL_REALM,
    RG.POSSIBLY_EXTINCT,
    RG.POSSIBLY_EXTINCT_IN_WILD
FROM {{ source('silver_source', 'stg_gator_attacks') }} GA
JOIN {{ source('silver_source', 'stg_red_list_gators') }} RG
    ON GA.SPECIES = RG.SCIENTIFIC_NAME