{{
    config(
        materialized='table',
        partition_by={
            'field': 'observation_date',
            'data_type': 'date',
            'granularity': 'month'
        }
    )
}}

/*
    EU HICP Year-over-Year from ECB data.
    The ECB series 'ICP.M.U2.N.000000.4.ANR' already reports YoY inflation.
*/

select
    observation_date,
    value as cpi_eu_yoy
from {{ ref('int_macro__ecb_daily') }}
where series_name = 'CPI_EU_YOY'

