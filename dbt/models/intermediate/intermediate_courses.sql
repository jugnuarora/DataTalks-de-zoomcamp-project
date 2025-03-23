{{
    config(
        materialized='view'
    )
}}

WITH formacode_union AS
(
    SELECT 
        fcod_1 as formacode, 
        COUNT(training_id) as trianing_count,
        COUNT(DISTINCT COALESCE(provider, provider_id)) AS provider_count,
        COUNT(distinct certification_title) as certification_count
    FROM
        {{ref('stg_courses')}}
    WHERE fcod_1 is not null
    GROUP BY 1
    UNION ALL
    SELECT 
        fcod_2 as formacode, 
        COUNT(training_id) as trianing_count,
        COUNT(DISTINCT COALESCE(provider, provider_id)) AS provider_count,
        COUNT(distinct certification_title) as certification_count
    FROM
        {{ref('stg_courses')}}
    WHERE fcod_2 is not null
    GROUP BY 1
    UNION ALL
    SELECT 
        fcod_3 as formacode, 
        COUNT(training_id) as trianing_count,
        COUNT(DISTINCT COALESCE(provider, provider_id)) AS provider_count,
        COUNT(distinct certification_title) as certification_count
    FROM
        {{ref('stg_courses')}}
    WHERE fcod_3 is not null
    GROUP BY 1
    UNION ALL
    SELECT 
        fcod_4 as formacode, 
        COUNT(training_id) as trianing_count,
        COUNT(DISTINCT COALESCE(provider, provider_id)) AS provider_count,
        COUNT(distinct certification_title) as certification_count
    FROM
        {{ref('stg_courses')}}
    WHERE fcod_4 is not null
    GROUP BY 1
    UNION ALL
    SELECT 
        fcod_5 as formacode, 
        COUNT(training_id) as trianing_count,
        COUNT(DISTINCT COALESCE(provider, provider_id)) AS provider_count,
        COUNT(distinct certification_title) as certification_count
    FROM
        {{ref('stg_courses')}}
    WHERE fcod_5 is not null
    GROUP BY 1

)
SELECT 
    formacode, 
    SUM(trianing_count) as total_nb_trianings,
    SUM(provider_count) as total_nb_providers,
    SUM(certification_count) as total_nb_certifications
FROM formacode_union
GROUP BY 1
ORDER BY 2 DESC