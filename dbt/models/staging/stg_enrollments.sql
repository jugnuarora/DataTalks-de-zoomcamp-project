{{
    config(
        materialized='view'
    )
}}

SELECT
    e.year_month,
    e.code_rncp,
    e.code_rs,
    
