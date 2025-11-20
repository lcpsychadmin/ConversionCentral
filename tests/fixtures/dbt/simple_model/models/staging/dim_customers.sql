{{ config(materialized='view', tags=['staging', 'dimensions']) }}

with
    source_0 as (
        select *
        from {{ source('crm', 'crm_customers') }}
    )

select *
from source_0
