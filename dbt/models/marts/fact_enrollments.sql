{{
    config(
        materialized='table'
    )
}}

SELECT 
    year_month,
    fu.formacode, 
    total_enrollments,
    total_nb_providers,
    total_nb_certifications,
    ROUND(total_enrollments / total_nb_providers, 1) AS enrollments_provider_ratio,
    f.description_en as formacode_description,
    f.field_en as formacode_field
FROM {{ref('prep_enrollments')}} fu
    LEFT JOIN {{ ref('dim_formacode') }} f ON cast(fu.formacode as STRING) = f.formacode
ORDER BY enrollments_provider_ratio DESC