{{config(
    materialized='table',
    tags= ['models', 'gold', 'sharks', 'monthly'],
    schema='GOLD'
)}}

SELECT DISTINCT
    SA.ATTACK_ID,
    SA.DATE_CLEAN AS ATTACK_DATE,
    SA.YEAR_CLEAN AS ATTACK_YEAR,
    SA.ATTACK_TYPE,
    SA.COUNTRY_CLEAN AS ATTACK_COUNTRY,
    SA.FATAL_OUTCOME,
    SL.AREA AS ATTACK_AREA,
    SL.LOCATION AS ATTACK_LOCATION,
    SL.SPECIES AS ATTACK_SPECIES,
    RS.ASSESSMENT_ID,
    RS.TAXON_ID,
    RS.SCIENTIFIC_NAME,
    RS.ORDER_NAME,
    RS.GENUS_NAME,
    RS.SPECIES_NAME AS RED_LIST_SPECIES_NAME,
    RS.COMMON_NAME,
    RS.RED_LIST_CATEGORY,
    RS.YEAR_PUBLISHED AS RED_LIST_YEAR,
    RS.ASSESSMENT_DATE,
    RS.HABITAT,
    RS.THREATS,
    RS.POPULATION,
    RS.POPULATION_TREND,
    RS.GLOBAL_LOCATION_RANGE,
    RS.ENVIRONMENT,
    RS.GLOBAL_REALM,
    RS.POSSIBLY_EXTINCT,
    RS.POSSIBLY_EXTINCT_IN_WILD
FROM {{ source('silver_source', 'stg_shark_attacks') }} SA
JOIN {{ source('silver_source', 'stg_shark_species_locations') }} SL
ON SA.ATTACK_ID = SL.ATTACK_ID
JOIN {{ source('silver_source', 'stg_red_list_sharks') }} RS
ON SL.SPECIES = RS.COMMON_NAME