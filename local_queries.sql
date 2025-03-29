
-------------------------------------------------------------------------COURSES RAW-------------------------------------------------------------------------

CREATE OR REPLACE EXTERNAL TABLE `courses.courses_raw`
OPTIONS (
  format = 'parquet',
  uris = ['gs://jugnu-france-course-enrollments/courses_enrol_data_2025_03_28/courses_raw_parquet/*.parquet'] -- change the date
);

select *
from `courses.courses_raw`
limit 10;

SELECT
    COLUMN_NAME,
    DATA_TYPE
FROM
    courses.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_NAME = "courses_raw";

--------------------------------------------------------------------------COURSES FILTERED--------------------------------------------------------------------

CREATE OR REPLACE EXTERNAL TABLE `courses.courses_filtered`
OPTIONS (
  format = 'parquet',
  uris = ['gs://jugnu-france-course-enrollments/courses_enrol_data_2025_03_28/courses_filtered/*.parquet'] --change the date
);

select *
from `courses.courses_filtered`
limit 10;

SELECT
    COLUMN_NAME,
    DATA_TYPE
FROM
    courses.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_NAME = "courses_filtered"; 

---------------------------------------------------------------------------SOURCE TABLE COURSES-------------------------------------------------------

select *
from `source_tables.courses`
limit 10;

SELECT
    COLUMN_NAME,
    DATA_TYPE
FROM
    source_tables.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_NAME = "courses"; 

------------------------------------------------------------------------------ Table with Courses raw, fileterd and source_tables count reconciliation ---------------------------------------------------------------------------
with courses_raw_cte as
(
  select 'All' as field,
  count(*) as raw_count
  from `courses.courses_raw`
  UNION ALL
  select 'code_formacode_1' as field,
  count(*) as raw_count
  from `courses.courses_raw`
  where code_formacode_1 is not null
  UNION ALL
  select 'department' as field,
  count(*) as raw_count
  from `courses.courses_raw`
  where nom_departement is not null
  UNION ALL
  select 'code_rs' as field,
  count(*) as raw_count
  from `courses.courses_raw`
  where code_inventaire is not null and code_inventaire != 1
  UNION ALL
  select 'code_rncp' as field,
  count(*) as raw_count
  from `courses.courses_raw`
  where code_rncp is not null and code_rncp != 1
  UNION ALL
  select 'code_certification' as field,
  count(*) as raw_count
  from `courses.courses_raw`
  where code_certifinfo is not null
  UNION ALL
  select 'provider_id' as field,
  count(*) as raw_count
  from `courses.courses_raw`
  where siret is not null
  UNION ALL
  select 'certification_title' as field,
  count(*) as raw_count
  from `courses.courses_raw`
  where intitule_certification is not null
  UNION ALL
  select 'code_formacode_1_selected' as field,
  count(*) as raw_count
  from `courses.courses_raw`
  where code_formacode_1 IN (31023, 31025, 31026)
),
courses_filtered_cte AS
(
  select 'All' as field,
  count(*) as filtered_count
  from `courses.courses_filtered`
  UNION ALL
  select 'code_formacode_1' as field,
  count(*) as filtered_count
  from `courses.courses_filtered`
  where code_formacode_1 is not null
  UNION ALL
  select 'department' as field,
  count(*) as filtered_count
  from `courses.courses_filtered`
  where nom_departement is not null
  UNION ALL
  select 'code_rs' as field,
  count(*) as filtered_count
  from `courses.courses_filtered`
  where code_inventaire is not null and code_inventaire != 1
  UNION ALL
  select 'code_rncp' as field,
  count(*) as filtered_count
  from `courses.courses_filtered`
  where code_rncp is not null and code_rncp != 1
  UNION ALL
  select 'code_certification' as field,
  count(*) as filtered_count
  from `courses.courses_filtered`
  where code_certifinfo is not null
  UNION ALL
  select 'provider_id' as field,
  count(*) as filtered_count
  from `courses.courses_filtered`
  where siret is not null
  UNION ALL
  select 'certification_title' as field,
  count(*) as filtered_count
  from `courses.courses_filtered`
  where intitule_certification is not null
  UNION ALL
  select 'code_formacode_1_selected' as field,
  count(*) as filtered_count
  from `courses.courses_filtered`
  where code_formacode_1 IN (31023, 31025, 31026)
),
courses_source_cte as
(
    select 'All' as field,
  count(*) as source_table_count
  from `source_tables.courses`
  UNION ALL
  select 'code_formacode_1' as field,
  count(*) as source_table_count
  from `source_tables.courses`
  where code_formacode_1 is not null
  UNION ALL
  select 'department' as field,
  count(*) as source_table_count
  from `source_tables.courses`
  where department is not null
  UNION ALL
  select 'code_rs' as field,
  count(*) as source_table_count
  from `source_tables.courses`
  where code_rs is not null and code_rs != 1
  UNION ALL
  select 'code_rncp' as field,
  count(*) as source_table_count
  from `source_tables.courses`
  where code_rncp is not null and code_rncp != 1
  UNION ALL
  select 'code_certification' as field,
  count(*) as source_table_count
  from `source_tables.courses`
  where code_certification is not null
  UNION ALL
  select 'provider_id' as field,
  count(*) as source_table_count
  from `source_tables.courses`
  where provider_id is not null
  UNION ALL
  select 'certification_title' as field,
  count(*) as source_table_count
  from `source_tables.courses`
  where certification_title is not null
  UNION ALL
  select 'code_formacode_1_selected' as field,
  count(*) as source_table_count
  from `source_tables.courses`
  where code_formacode_1 IN (31023, 31025, 31026)
)
select
  field,
  raw_count,
  filtered_count,
  source_table_count
from
  courses_raw_cte
  FULL JOIN courses_filtered_cte USING (field)
  FULL JOIN courses_source_cte USING (field);

----------------------------------------------------------------------------ENROLLMENTS RAW------------------------------------------------------------

CREATE OR REPLACE EXTERNAL TABLE `enrollments.enrollments_raw`
OPTIONS (
  format = 'parquet',
  uris = ['gs://jugnu-france-course-enrollments/courses_enrol_data_2025_03_28/enrollments_raw_parquet/*.parquet'] -- change the date
);

select *
from `enrollments.enrollments_raw`
limit 10;

SELECT
    COLUMN_NAME,
    DATA_TYPE
FROM
    enrollments.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_NAME = "enrollments_raw";

----------------------------------------------------------------------------ENROLLMENTS FILTERED------------------------------------------------------

CREATE OR REPLACE EXTERNAL TABLE `enrollments.enrollments_filtered`
OPTIONS (
  format = 'parquet',
  uris = ['gs://jugnu-france-course-enrollments/courses_enrol_data_2025_03_28/enrollments_filtered/*.parquet'] -- change the date
);

select *
from `enrollments.enrollments_filtered`
limit 10;

SELECT
    COLUMN_NAME,
    DATA_TYPE
FROM
    enrollments.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_NAME = "enrollments_filtered";

----------------------------------------------------------------------------SOURCE TABLE ENROLLMENTS---------------------------------------------------

select *
from `source_tables.enrollments`
limit 10;

SELECT
    COLUMN_NAME,
    DATA_TYPE
FROM
    source_tables.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_NAME = "enrollments";

------------------------------------------------------------------------------ Table with Enrollments raw, fileterd and source_tables count reconciliation ---------------------------------------------------------------------------
with enrollments_raw_cte as
(
  select 'All' as field,
  count(*) as raw_count
  from `enrollments.enrollments_raw`
  UNION ALL
  select 'year_month' as field,
  count(*) as raw_count
  from `enrollments.enrollments_raw`
  where annee_mois is not null
  UNION ALL
  select 'code_rs' as field,
  count(*) as raw_count
  from `enrollments.enrollments_raw`
  where code_rs is not null and code_rs != -1
  UNION ALL
  select 'code_rncp' as field,
  count(*) as raw_count
  from `enrollments.enrollments_raw`
  where code_rncp is not null and code_rncp != 1
  UNION ALL
  select 'code_certification' as field,
  count(*) as raw_count
  from `enrollments.enrollments_raw`
  where code_certifinfo is not null
  UNION ALL
  select 'provider_id' as field,
  count(*) as raw_count
  from `enrollments.enrollments_raw`
  where siret_of_contractant is not null
  UNION ALL
  select 'certification_title' as field,
  count(*) as raw_count
  from `enrollments.enrollments_raw`
  where intitule_certification is not null
  UNION ALL
  select 'training_entries' as field,
  count(*) as raw_count
  from `enrollments.enrollments_raw`
  where entrees_formation != 0
),
enrollments_filtered_cte AS
(
  select 'All' as field,
  count(*) as filtered_count
  from `enrollments.enrollments_filtered`
  UNION ALL
  select 'year_month' as field,
  count(*) as filtered_count
  from `enrollments.enrollments_filtered`
  where annee_mois is not null
  UNION ALL
  select 'code_rs' as field,
  count(*) as filtered_count
  from `enrollments.enrollments_filtered`
  where code_rs is not null and code_rs != -1
  UNION ALL
  select 'code_rncp' as field,
  count(*) as filtered_count
  from `enrollments.enrollments_filtered`
  where code_rncp is not null and code_rncp != 1
  UNION ALL
  select 'code_certification' as field,
  count(*) as filtered_count
  from `enrollments.enrollments_filtered`
  where code_certifinfo is not null
  UNION ALL
  select 'provider_id' as field,
  count(*) as filtered_count
  from `enrollments.enrollments_filtered`
  where siret_of_contractant is not null
  UNION ALL
  select 'certification_title' as field,
  count(*) as filtered_count
  from `enrollments.enrollments_filtered`
  where intitule_certification is not null
  UNION ALL
  select 'training_entries' as field,
  count(*) as filtered_count
  from `enrollments.enrollments_filtered`
  where entrees_formation != 0
),
enrollments_source_cte as
(
  select 'All' as field,
  count(*) as source_table_count
  from `source_tables.enrollments`
  UNION ALL
  select 'year_month' as field,
  count(*) as source_table_count
  from `source_tables.enrollments`
  where year_month is not null
  UNION ALL
  select 'code_rs' as field,
  count(*) as source_table_count
  from `source_tables.enrollments`
  where code_rs is not null and code_rs != -1
  UNION ALL
  select 'code_rncp' as field,
  count(*) as source_table_count
  from `source_tables.enrollments`
  where code_rncp is not null and code_rncp != 1
  UNION ALL
  select 'code_certification' as field,
  count(*) as source_table_count
  from `source_tables.enrollments`
  where code_certification is not null
  UNION ALL
  select 'provider_id' as field,
  count(*) as source_table_count
  from `source_tables.enrollments`
  where provider_id is not null
  UNION ALL
  select 'certification_title' as field,
  count(*) as source_table_count
  from `source_tables.enrollments`
  where certification_title is not null
  UNION ALL
  select 'training_entries' as field,
  count(*) as source_table_count
  from `source_tables.enrollments`
  where training_entries != 0
)
select
  field,
  raw_count,
  filtered_count,
  source_table_count
from
  enrollments_raw_cte
  FULL JOIN enrollments_filtered_cte USING (field)
  FULL JOIN enrollments_source_cte USING (field);

----------------------------------------------------------------------------FORMACODE GCS----------------------------------------------------------------

CREATE OR REPLACE EXTERNAL TABLE `courses.formacode_translated`
OPTIONS (
  format = 'parquet',
  uris = ['gs://jugnu-france-course-enrollments/formacode_translated/*.parquet']
);

select *
from `conciliation.formacode_translated`
--where formacode IN (31023, 31025, 31026)
limit 10;

SELECT
    COLUMN_NAME,
    DATA_TYPE
FROM
    conciliation.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_NAME = "formacode_translated";

---------------------------------------------------------------------------SOURCE TABLE FORMACODE---------------------------------------------------------------

select *
from `source_tables.formacode`
--where formacode IN (31023, 31025, 31026)
limit 10;

SELECT
    COLUMN_NAME,
    DATA_TYPE
FROM
    source_tables.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_NAME = "formacode";

------------------------------------------------------------------------------ Table with Formacode GCS and source_tables count reconciliation ---------------------------------------------------------------------------
with formacode_gcs as
(
  select 'All' as field,
  count(*) as raw_count
  from `courses.formacode_translated`
  UNION ALL
  select 'description' as field,
  count(*) as raw_count
  from `courses.formacode_translated`
  where formacode is not null
  UNION ALL
  select 'field' as field,
  count(*) as raw_count
  from `courses.formacode_translated`
  where field is not null
  UNION ALL
  select 'generic_term' as field,
  count(*) as raw_count
  from `courses.formacode_translated`
  where generic_term is not null
  UNION ALL
  select 'description_en' as field,
  count(*) as raw_count
  from `courses.formacode_translated`
  where description_en is not null
  UNION ALL
  select 'field_en' as field,
  count(*) as raw_count
  from `courses.formacode_translated`
  where field_en is not null
),
formacode_source_cte as
(
  select 'All' as field,
  count(*) as source_table_count
  from `source_tables.formacode`
  UNION ALL
  select 'description' as field,
  count(*) as source_table_count
  from `source_tables.formacode`
  where formacode is not null
  UNION ALL
  select 'field' as field,
  count(*) as source_table_count
  from `source_tables.formacode`
  where field is not null
  UNION ALL
  select 'generic_term' as field,
  count(*) as source_table_count
  from `source_tables.formacode`
  where generic_term is not null
  UNION ALL
  select 'description_en' as field,
  count(*) as source_table_count
  from `source_tables.formacode`
  where description_en is not null
  UNION ALL
  select 'field_en' as field,
  count(*) as source_table_count
  from `source_tables.formacode`
  where field_en is not null
)
select
  field,
  raw_count,
  source_table_count
from
  formacode_gcs
  FULL JOIN formacode_source_cte USING (field);

------------------------------------------------------------------------MISC COURSES---------------------------------------------------------------------------


select count(*)
from
(
select provider, department, certification_title, code_formacode_1, code_formacode_2, code_formacode_3, code_formacode_4, code_formacode_5, code_certification, provider_id, training_id, training_title  
from `staging.courses`
EXCEPT DISTINCT
select provider, department, certification_title, code_formacode_1, code_formacode_2, code_formacode_3, code_formacode_4, code_formacode_5, code_certification, provider_id, training_id, training_title 
from `source_tables.courses`
);


select count(*)
from `dbt_models_staging.stg_courses`
where code_formacode_3 != code_formacode_5;

select * 
from `dbt_models.intermediate_courses`
where formacode IN ('31023', '31025', '31026');

SELECT 
  code_formacode_1 as formacode, 
  count(training_id) as trianing_count,
  COUNT(DISTINCT provider_id),
  COUNT(DISTINCT CASE WHEN provider IS NULL THEN provider_id ELSE provider END) AS provider_count,
  COUNT(DISTINCT COALESCE(provider, provider_id)) AS provider_count_co,
  count(distinct certification_title) as certification_count
FROM
  `dbt_models_staging.stg_courses`
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

select count(distinct training_id) as dist_formacode
select *
from `source_tables.courses`
where code_certification != '-1'
where code_formacode_1 is not null
group by provider, provider_id, certification_title, code_certification, code_rncp, code_rs, code_formacode_1
having dist_formacode > 1
limit 2;

select count(*)
from `dbt_models_staging.stg_courses`
limit 3;

select count(*)
select count(distinct formacode)
from `dbt_models_intermediate.intermediate_courses`

select *
select count(*)
from `dbt_models_intermediate.intermediate_enrollments`
where formacode = '31025'
limit 3

--------------------------------------------------------------------------------------MISC ENROLLMENTS-------------------------------------------------------------------

select count(*)
--select *
from `dbt_models_staging.stg_enrollments`
where code_formacode_1 is not null

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
        `source_tables.enrollments`
),
enrollments_formacode AS
(
  SELECT
    e.*, c.code_formacode_1, c.code_formacode_2, c.code_formacode_3, c.code_formacode_4, c.code_formacode_5
FROM
    enrollments_data AS e
    LEFT JOIN `dbt_models_staging.stg_courses` as c
    on (lower(e.provider) = lower(c.provider)
        AND e.provider_id = c.provider_id
        AND e.certification_title = c.certification_title
        AND e.code_certification = c.code_certification
        AND e.code_rncp = c.code_rncp
        AND e.code_rs = c.code_rs)
--where c.code_formacode_1 is not null
),
rn_enrollments as
--with rn_enrollments as
(
  select *,
    row_number() over(partition by year_month, code_rncp, code_rs, certification_title, code_certification, provider_id, provider, code_formacode_1, code_formacode_2, code_formacode_3, code_formacode_4, code_formacode_5) as rn
  from enrollments_formacode
  --from `source_tables.enrollments`
)
select count(*)
from rn_enrollments
where rn = 1 #and code_formacode_1 is not null and training_entries != 0
LIMIT 10;
