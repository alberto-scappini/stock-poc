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
    Credit spread indicators from FRED.
    Includes High Yield and Investment Grade OAS spreads.
*/

with spreads_long as (

    select
        observation_date,
        series_id,
        value
    from {{ ref('int_macro__fred_daily') }}
    where series_id in (
        'T10Y3M',           -- 10Y-3M Treasury Spread
        'TEDRATE'           -- TED Spread
    )

)

select
    observation_date,
    max(case when series_id = 'T10Y3M' then value end) as us_10y3m_spread,
    max(case when series_id = 'TEDRATE' then value end) as ted_spread

from spreads_long
group by observation_date

