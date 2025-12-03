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
    Daily ETF yield features based on trailing dividend payments.
    Calculates 3M, 6M, and 12M trailing yields.
    For accumulating ETFs, yields will be 0/null.
    
    Note: Using ROWS BETWEEN with day counts instead of RANGE BETWEEN INTERVAL
    because BigQuery/dbt-fusion doesn't support interval in window frames.
    Approximation: 252 trading days ~ 12 months, 126 ~ 6 months, 63 ~ 3 months
*/

with base as (

    select
        p.price_date,
        p.symbol,
        p.close_price,
        d.div_per_share_day as div_per_share_day
    from {{ ref('int_prices__etf_daily') }} p
    left join {{ ref('int_dividends__daily_sum') }} d
        on p.price_date = d.dividend_date
        and p.symbol = d.symbol

),

div_roll as (

    select
        price_date,
        symbol,
        close_price,
        div_per_share_day,
        
        -- 12M trailing sum of dividends (~252 trading days)
        sum(div_per_share_day) over (
            partition by symbol
            order by price_date
            rows between 252 preceding and current row
        ) as div_12m,
        
        -- 6M trailing sum of dividends (~126 trading days)
        sum(div_per_share_day) over (
            partition by symbol
            order by price_date
            rows between 126 preceding and current row
        ) as div_6m,
        
        -- 3M trailing sum of dividends (~63 trading days)
        sum(div_per_share_day) over (
            partition by symbol
            order by price_date
            rows between 63 preceding and current row
        ) as div_3m

    from base

)

select
    price_date,
    symbol,
    close_price,
    div_12m,
    div_6m,
    div_3m,
    
    -- TTM yield (trailing 12 months)
    case 
        when close_price > 0 then div_12m / close_price 
        else null 
    end as yield_12m_ttm,
    
    -- Annualized 6M yield
    case 
        when close_price > 0 then div_6m / close_price * (365.0 / 180.0) 
        else null 
    end as yield_6m_ann,
    
    -- Annualized 3M yield  
    case 
        when close_price > 0 then div_3m / close_price * (365.0 / 90.0) 
        else null 
    end as yield_3m_ann

from div_roll
