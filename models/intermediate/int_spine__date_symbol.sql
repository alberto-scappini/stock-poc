{{
    config(
        materialized='table',
        partition_by={
            'field': 'date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['symbol']
    )
}}

/*
    Date-Symbol spine derived from actual price data.
    
    NOTE: This spine is based on symbols that have prices, not dim_symbol.
    This ensures we have a complete grid of all traded dates x symbols.
    
    The fact table uses prices directly as the driver, but this spine
    can be useful for analysis and data quality checks.
*/

with date_range as (

    select
        min(price_date) as min_date,
        max(price_date) as max_date
    from {{ ref('int_prices__etf_daily') }}

),

dates as (

    select date
    from date_range,
    unnest(generate_date_array(min_date, max_date)) as date

),

-- Get unique symbols from prices (not dim_symbol)
symbols as (

    select distinct symbol
    from {{ ref('int_prices__etf_daily') }}

)

select
    d.date,
    s.symbol
from dates d
cross join symbols s
