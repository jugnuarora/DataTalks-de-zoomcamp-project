{{
    config(
        materialized='table'
    )
}}

SELECT 
    formacode,
    description_en,
    {{remove_leading_numbers('field_en')}} AS field_en
FROM {{ source('staging', 'formacode') }} f