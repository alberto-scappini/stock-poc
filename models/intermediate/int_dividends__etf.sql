{{
    config(
        materialized='table',
        partition_by={
            'field': 'payment_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['symbol']
    )
}}

/*
    ETF dividend data normalized.
    
    IMPORTANT: This table includes ALL dividends from stg_div_eod_div.
    It does NOT filter by dim_symbol - this ensures we capture all dividend data
    even if symbol metadata is incomplete.
    
    Used to calculate trailing yield features.
*/

select
    payment_date,
    symbol,
    safe_cast(dividend_value as float64) as dividend_per_share,
    period
from {{ ref('stg_div_eod_div') }}
where payment_date is not null
  and dividend_value is not null
  and safe_cast(dividend_value as float64) > 0
