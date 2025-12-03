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
    US CPI Year-over-Year calculation from FRED CPIAUCSL series.
    CPI is reported monthly, so we calculate YoY change.
*/

with cpi as (

    select
        observation_date,
        value as cpi_index
    from {{ ref('int_macro__fred_daily') }}
    where series_id = 'CPIAUCSL'

),

with_yoy as (

    select
        observation_date,
        cpi_index,
        lag(cpi_index, 12) over (order by observation_date) as cpi_index_lag_12m

    from cpi

)

select
    observation_date,
    cpi_index,
    safe_divide(cpi_index - cpi_index_lag_12m, cpi_index_lag_12m) * 100 as cpi_us_yoy

from with_yoy
where cpi_index_lag_12m is not null

