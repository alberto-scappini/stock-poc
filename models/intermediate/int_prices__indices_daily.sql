{{
    config(
        materialized='table',
        partition_by={
            'field': 'price_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['symbol']
    )
}}

/*
    Daily index and FX prices for risk proxies and market indicators.
    These will be used as macro regime features in the final model.
    
    Available symbols (from indx_eod_prices):
    - GSPC.INDX (S&P 500)
    - SX5E.INDX (Euro Stoxx 50)
    - VIX.INDX (Volatility Index)
    - DXY.INDX (US Dollar Index)
    - EURUSD.FOREX (EUR/USD exchange rate)
*/

with indices as (

    select
        price_date,
        symbol,
        close_price
    from {{ ref('stg_indx_eod_prices') }}
    where symbol in (
        -- Volatility index
        'VIX.INDX',
        
        -- Currency indices
        'DXY.INDX',         -- US Dollar Index
        'EURUSD.FOREX',     -- EUR/USD exchange rate
        
        -- Major equity indices
        'GSPC.INDX',        -- S&P 500
        'SX5E.INDX'         -- Euro Stoxx 50
    )

),

with_returns as (

    select
        price_date,
        symbol,
        close_price,
        
        -- Daily return
        safe_divide(
            close_price - lag(close_price) over (partition by symbol order by price_date),
            lag(close_price) over (partition by symbol order by price_date)
        ) as return_1d,
        
        -- Change from prior periods
        lag(close_price, 5) over (partition by symbol order by price_date) as close_lag_1w,
        lag(close_price, 21) over (partition by symbol order by price_date) as close_lag_1m

    from indices

),

with_changes as (

    select
        price_date,
        symbol,
        close_price,
        return_1d,
        safe_divide(close_price - close_lag_1w, close_lag_1w) as change_1w,
        safe_divide(close_price - close_lag_1m, close_lag_1m) as change_1m

    from with_returns

)

select * from with_changes
