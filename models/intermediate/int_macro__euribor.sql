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
    Euribor rates from ECB data.
    Pivoted to have each tenor as a column.
*/

with euribor_long as (

    select
        observation_date,
        series_name,
        value
    from {{ ref('int_macro__ecb_daily') }}
    where series_name in ('EURIBOR_3M', 'EURIBOR_6M', 'EURIBOR_12M')

)

select
    observation_date,
    max(case when series_name = 'EURIBOR_3M' then value end) as euribor_3m,
    max(case when series_name = 'EURIBOR_6M' then value end) as euribor_6m,
    max(case when series_name = 'EURIBOR_12M' then value end) as euribor_12m

from euribor_long
group by observation_date

