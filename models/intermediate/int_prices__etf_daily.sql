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
    Daily ETF prices with basic price features.
    
    IMPORTANT: This table includes ALL prices from stg_eod_prices.
    It does NOT filter by dim_symbol - this ensures we capture all price data
    even if symbol metadata is incomplete.
    
    The fact table will LEFT JOIN to dim_symbol for metadata.
*/

with prices as (

    select
        p.price_date,
        p.symbol,
        p.open_price,
        p.high_price,
        p.low_price,
        p.close_price,
        p.adjusted_close,
        p.volume
    from {{ ref('stg_eod_prices') }} p

),

with_returns as (

    select
        price_date,
        symbol,
        open_price,
        high_price,
        low_price,
        close_price,
        adjusted_close,
        volume,
        
        -- Daily return
        safe_divide(
            adjusted_close - lag(adjusted_close) over (partition by symbol order by price_date),
            lag(adjusted_close) over (partition by symbol order by price_date)
        ) as return_1d,
        
        -- Lagged prices for momentum calculations
        lag(adjusted_close, 1) over (partition by symbol order by price_date) as adj_close_lag_1d,
        lag(adjusted_close, 5) over (partition by symbol order by price_date) as adj_close_lag_1w,
        lag(adjusted_close, 21) over (partition by symbol order by price_date) as adj_close_lag_1m,
        lag(adjusted_close, 63) over (partition by symbol order by price_date) as adj_close_lag_3m,
        lag(adjusted_close, 126) over (partition by symbol order by price_date) as adj_close_lag_6m,
        lag(adjusted_close, 252) over (partition by symbol order by price_date) as adj_close_lag_12m

    from prices

),

with_momentum as (

    select
        price_date,
        symbol,
        open_price,
        high_price,
        low_price,
        close_price,
        adjusted_close,
        volume,
        return_1d,
        
        -- Momentum features (returns over different periods)
        safe_divide(adjusted_close - adj_close_lag_1w, adj_close_lag_1w) as return_1w,
        safe_divide(adjusted_close - adj_close_lag_1m, adj_close_lag_1m) as return_1m,
        safe_divide(adjusted_close - adj_close_lag_3m, adj_close_lag_3m) as return_3m,
        safe_divide(adjusted_close - adj_close_lag_6m, adj_close_lag_6m) as return_6m,
        safe_divide(adjusted_close - adj_close_lag_12m, adj_close_lag_12m) as return_12m,
        
        -- Volatility (rolling 21-day std dev of returns)
        stddev(return_1d) over (
            partition by symbol 
            order by price_date 
            rows between 20 preceding and current row
        ) as volatility_21d,
        
        -- Volatility (rolling 63-day std dev of returns)
        stddev(return_1d) over (
            partition by symbol 
            order by price_date 
            rows between 62 preceding and current row
        ) as volatility_63d

    from with_returns

)

select * from with_momentum
