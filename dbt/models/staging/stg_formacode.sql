{{
    config(
        materialized='view'
    )
}}

SELECT 
    f.formacode,
    description_en,
    REGEXP_REPLACE(f.field_en, r'^\d+\s+', '') AS field_en
FROM {{ source('staging_tmp', 'formacode') }} f