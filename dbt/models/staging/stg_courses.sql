{{
    config(
        materialized='view'
    )
}}

WITH courses_data as
(
    SELECT
        FORMAT_DATE("%Y-%m", {{ dbt.date_trunc("month", "date_extract") }}) AS course_month,
        date_extract,
        provider,
        department,
        certification_title,
        code_certification,
        code_rncp,
        code_rs,
        code_formacode_1,
        code_formacode_2,
        code_formacode_3,
        code_formacode_4,
        code_formacode_5,
        provider_id,
        training_id
    FROM
        {{ source('staging', 'courses') }}
    WHERE 
        code_formacode_1 IS NOT NULL
),
rn_courses as
(
  select *,
    row_number() over(partition by course_month, training_id, department ORDER BY date_extract DESC) as rn
  from courses_data
)
SELECT
    course_month, 
    date_extract,
    provider,
    certification_title,
    code_certification,
    code_rncp,
    code_rs,
    code_formacode_1,
    code_formacode_2,
    code_formacode_3,
    code_formacode_4,
    code_formacode_5,
    provider_id,
    training_id
FROM
    rn_courses
WHERE rn=1
    
{% if var('limit_data', env_var('DBT_ENVIRONMENT') == 'development') %}

  limit 100

{% endif %}