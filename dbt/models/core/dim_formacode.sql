{{
    config(
        materialized='table'
    )
}}

SELECT 
    formacode,
    description_en,
    REGEXP_REPLACE(f.field_en, r'^\d+\s+', '') AS field_en
FROM {{ source('staging', 'formacode') }} f