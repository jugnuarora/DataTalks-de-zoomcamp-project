{{
    config(
        materialized='table'
    )
}}

SELECT 
    fu.formacode, 
    total_nb_trianings,
    total_nb_providers,
    total_nb_certifications,
    ROUND(total_nb_trianings / total_nb_providers, 1) AS training_provider_ratio,
    f.translation as formacode_description
FROM {{ref('intermediate_courses')}} fu
    LEFT JOIN {{ source('staging', 'formacode') }} f ON fu.formacode = f.formacode
ORDER BY training_provider_ratio DESC