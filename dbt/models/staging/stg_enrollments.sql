{{
    config(
        materialized='view'
    )
}}

with enrollments_data as
(
    SELECT
        year_month,
        code_rncp,
        code_rs,
        certification_title,
        code_certification,
        provider_id,
        provider,
        training_entries
    FROM 
        {{source("staging", "enrollments")}}
),
enrollments_formacode AS
(
  SELECT
    e.*, c.code_formacode_1, c.code_formacode_2, c.code_formacode_3, c.code_formacode_4, c.code_formacode_5
FROM
    enrollments_data AS e
    LEFT JOIN {{ref('stg_courses')}} as c
    on (lower(e.provider) = lower(c.provider)
        AND e.provider_id = c.provider_id
        AND lower(e.certification_title) = lower(c.certification_title)
        AND e.code_certification = c.code_certification
        AND e.code_rncp = c.code_rncp
        AND e.code_rs = c.code_rs)
),
rn_enrollments as
(
  select *,
    row_number() over(partition by year_month, code_rncp, code_rs, certification_title, code_certification, provider_id, provider, code_formacode_1, code_formacode_2, code_formacode_3, code_formacode_4, code_formacode_5) as rn
  from enrollments_formacode
)
select *
from rn_enrollments
where rn = 1 and code_formacode_1 is not null and training_entries != 0