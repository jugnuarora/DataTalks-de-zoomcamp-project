{{
    config(
        materialized='table'
    )
}}

SELECT 
    CAST(formacode AS INTEGER) as formacode,
    description_en,
    {{remove_leading_numbers('field_en')}} AS field_en
FROM {{ source('staging', 'formacode') }} f