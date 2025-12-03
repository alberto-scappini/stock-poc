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
    US policy rates from FRED data.
    Pivoted to have each rate as a column.
*/

with rates_long as (

    select
        observation_date,
        series_id,
        value
    from {{ ref('int_macro__fred_daily') }}
    where series_id in ('EFFR', 'SOFR', 'DFF')

)

select
    observation_date,
    max(case when series_id = 'EFFR' then value end) as effr,
    max(case when series_id = 'SOFR' then value end) as sofr,
    max(case when series_id = 'DFF' then value end) as fed_funds_rate

from rates_long
group by observation_date

