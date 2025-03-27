{{
    config(
        materialized='view'
    )
}}

WITH formacode_union AS
(
    SELECT 
        course_month,
        code_formacode_1 as formacode, 
        COUNT(training_id) as trianing_count,
        COUNT(DISTINCT COALESCE(provider, cast(provider_id AS STRING))) AS provider_count,
        COUNT(distinct certification_title) as certification_count
    FROM
        {{ref('stg_courses')}}
    WHERE code_formacode_1 is not null
    GROUP BY 1, 2
    UNION ALL
    SELECT 
        course_month,
        code_formacode_2 as formacode, 
        COUNT(training_id) as trianing_count,
        COUNT(DISTINCT COALESCE(provider, cast(provider_id AS STRING))) AS provider_count,
        COUNT(distinct certification_title) as certification_count
    FROM
        {{ref('stg_courses')}}
    WHERE code_formacode_2 is not null
    GROUP BY 1, 2
    UNION ALL
    SELECT 
        course_month,
        code_formacode_3 as formacode, 
        COUNT(training_id) as trianing_count,
        COUNT(DISTINCT COALESCE(provider, cast(provider_id AS STRING))) AS provider_count,
        COUNT(distinct certification_title) as certification_count
    FROM
        {{ref('stg_courses')}}
    WHERE code_formacode_3 is not null
    GROUP BY 1, 2
    UNION ALL
    SELECT 
        course_month,
        code_formacode_4 as formacode, 
        COUNT(training_id) as trianing_count,
        COUNT(DISTINCT COALESCE(provider, cast(provider_id AS STRING))) AS provider_count,
        COUNT(distinct certification_title) as certification_count
    FROM
        {{ref('stg_courses')}}
    WHERE code_formacode_4 is not null
    GROUP BY 1, 2
    UNION ALL
    SELECT 
        course_month,
        code_formacode_5 as formacode, 
        COUNT(training_id) as trianing_count,
        COUNT(DISTINCT COALESCE(provider, cast(provider_id AS STRING))) AS provider_count,
        COUNT(distinct certification_title) as certification_count
    FROM
        {{ref('stg_courses')}}
    WHERE code_formacode_5 is not null
    GROUP BY 1, 2

)
SELECT 
    course_month,
    formacode, 
    SUM(trianing_count) as total_nb_trianings,
    SUM(provider_count) as total_nb_providers,
    SUM(certification_count) as total_nb_certifications
FROM formacode_union
GROUP BY 1, 2
ORDER BY 3 DESC