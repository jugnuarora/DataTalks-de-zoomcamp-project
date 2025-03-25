{{
    config(
        materialized='view'
    )
}}

WITH formacode_union AS
(
    SELECT 
        year_month,
        fcod_1 as formacode, 
        SUM(training_entries) as training_entries,
        COUNT(DISTINCT COALESCE(provider, provider_id)) AS provider_count,
        COUNT(distinct certification_title) as certification_count
    FROM
        {{ref('stg_enrollments')}}
    WHERE fcod_1 is not null
    GROUP BY 1, 2
    UNION ALL
    SELECT 
        year_month,
        fcod_2 as formacode, 
        SUM(training_entries) as training_entries,
        COUNT(DISTINCT COALESCE(provider, provider_id)) AS provider_count,
        COUNT(distinct certification_title) as certification_count
    FROM
        {{ref('stg_enrollments')}}
    WHERE fcod_2 is not null
    GROUP BY 1, 2
    UNION ALL
    SELECT 
        year_month,
        fcod_3 as formacode, 
        SUM(training_entries) as training_entries,
        COUNT(DISTINCT COALESCE(provider, provider_id)) AS provider_count,
        COUNT(distinct certification_title) as certification_count
    FROM
        {{ref('stg_enrollments')}}
    WHERE fcod_3 is not null
    GROUP BY 1, 2
    UNION ALL
    SELECT 
        year_month,
        fcod_4 as formacode, 
        SUM(training_entries) as training_entries,
        COUNT(DISTINCT COALESCE(provider, provider_id)) AS provider_count,
        COUNT(distinct certification_title) as certification_count
    FROM
        {{ref('stg_enrollments')}}
    WHERE fcod_4 is not null
    GROUP BY 1, 2
    UNION ALL
    SELECT 
        year_month,
        fcod_5 as formacode, 
        SUM(training_entries) as training_entries,
        COUNT(DISTINCT COALESCE(provider, provider_id)) AS provider_count,
        COUNT(distinct certification_title) as certification_count
    FROM
        {{ref('stg_enrollments')}}
    WHERE fcod_5 is not null
    GROUP BY 1, 2

)
SELECT 
    year_month,
    formacode, 
    SUM(training_entries) as total_enrollments,
    SUM(provider_count) as total_nb_providers,
    SUM(certification_count) as total_nb_certifications
FROM formacode_union
GROUP BY 1, 2
ORDER BY 3 DESC