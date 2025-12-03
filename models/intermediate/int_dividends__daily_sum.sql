{{
    config(
        materialized='table',
        partition_by={
            'field': 'dividend_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['symbol']
    )
}}

/*
    Aggregated daily dividend payments per symbol.
    Some symbols may have multiple dividend payments on the same day,
    so we sum them to simplify rolling window calculations.
*/

select
    payment_date as dividend_date,
    symbol,
    sum(dividend_per_share) as div_per_share_day
from {{ ref('int_dividends__etf') }}
group by 1, 2

