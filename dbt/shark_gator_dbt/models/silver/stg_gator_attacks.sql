{{config(
    materialized='view',
    tags= ['staging', 'silver', 'attacks', 'gators', 'monthly'],
)}}

SELECT DISTINCT
"INCIDENT_NUMBER" AS ATTACK_ID,
CASE "SPECIES"
            WHEN 'TSCH' THEN 'Tomistoma schlegelii'
            WHEN 'CYAC' THEN 'Caiman yacare'
            WHEN 'CLAT' THEN 'Caiman latirostris'
            WHEN 'CSIA' THEN 'Crocodylus siamensis'
            WHEN 'CACU' THEN 'Crocodylus acutus'
            WHEN 'MCAT' THEN 'Mecistops cataphractus'
            WHEN 'CPAL' THEN 'Crocodylus palustris'
            WHEN 'AMIS' THEN 'Alligator mississippiensis'
            WHEN 'CSUC' THEN 'Caiman crocodilus'
            WHEN 'MNIG' THEN 'Melanosuchus niger'
            WHEN 'CINT' THEN 'Crocodylus intermedius'
            WHEN 'CCRO' THEN 'Crocodylus rhombifer'
            WHEN 'CPOR' THEN 'Crocodylus porosus'
            WHEN 'CNIL' THEN 'Crocodylus niloticus'
            WHEN 'CMOR' THEN 'Crocodylus moreletii'
            WHEN 'CJOH' THEN 'Crocodylus johnstoni'
            WHEN 'PPAL' THEN 'Paleosuchus palpebrosus'
            WHEN 'ASIN' THEN 'Alligator sinensis'
            WHEN 'PTRI' THEN 'Paleosuchus trigonatus'
            WHEN 'CMIN' THEN 'Crocodylus mindorensis'
            WHEN 'CNOV' THEN 'Crocodylus novaeguineae'
            WHEN 'OTET' THEN 'Osteolaemus tetraspis'
            WHEN 'GGAN' THEN 'Gavialis gangeticus'
            ELSE NULL
        END AS SPECIES,
CAST("LATITUDE" AS FLOAT) AS LATITUDE,
CAST("LONGITUDE" AS FLOAT) AS LONGITUDE,
CASE
        WHEN "PROVINCE/STATE" IN ('Yucatan', 'Yucatán') THEN 'Yucatan'
        WHEN "PROVINCE/STATE" IN ('Michoacan', 'Michoacán') THEN 'Michoacan'
        WHEN "PROVINCE/STATE" IN ('Kasai-Oriental', 'Kasai Oriental') THEN 'Kasai-Oriental'
        WHEN "PROVINCE/STATE" IN ('Makira Ulawa', 'Makira-Ulawa') THEN 'Makira-Ulawa'
        WHEN "PROVINCE/STATE" IN ('North-West', 'North West', 'Northwestern', 'North-western') THEN 'North West'
        WHEN "PROVINCE/STATE" IN ('Sistan & Baluchestan', 'Sistan and Baluchestan') THEN 'Sistan and Baluchestan'
        WHEN "PROVINCE/STATE" IN ('Baoruco', 'Bahoruco') THEN 'Baoruco'
        ELSE "PROVINCE/STATE"
    END AS PROVINCE_STATE,
UPPER("COUNTRY") AS COUNTRY,
"OUTCOME" AS OUTCOME,
CASE
    WHEN "OUTCOME" = 'Fatal' THEN True
    WHEN "OUTCOME" = 'Non-fatal' THEN False
        END AS FATAL_OUTCOME,
"DATE" AS RAW_ATTACK_DATE,
COALESCE(
    TRY_TO_DATE("DATE", 'MMMM DD, YYYY'), NULL
) AS EXACT_ATTACK_DATE,
CASE
    WHEN REGEXP_COUNT("DATE", '\\d{4}') = 1 THEN TO_NUMBER(REGEXP_SUBSTR("DATE", '\\d{4}'))
    ELSE NULL
END AS ATTACK_YEAR,
"SOURCE_FILE" AS SOURCE_FILE,
"LOAD_TIMESTAMP_UTC" AS INGESTION_TIMESTAMP_UTC,
CURRENT_TIMESTAMP() AS CLEANED_TIMESTAMP_UTC
FROM {{ source('bronze_source', 'RAW_GATOR_ATTACKS') }}