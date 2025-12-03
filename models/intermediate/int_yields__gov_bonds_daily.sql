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
    Government bond yields for curve construction and spread calculations.
    The close_price in gbond represents the yield (not price).
    
    Available symbols (from gbond_eod_prices):
    US: US10Y, US5Y, US3Y, US1Y
    Germany: DE10Y, DE5Y, DE3Y, DE1Y
    Other EU: IT10Y, ES10Y, FR10Y, NL10Y
    EM: MX10Y, CN10Y, KR10Y, MY10Y, IN10Y, ID10Y, BR10Y, TH10Y, TR10Y
    
    Note: UK bonds NOT available, US2Y/US30Y NOT available
*/

with bonds as (

    select
        price_date,
        symbol,
        cast(close_price as float64) as yield
    from {{ ref('stg_gbond_eod_prices') }}
    where symbol in (
        -- US Treasury yields (available: 10Y, 5Y, 3Y, 1Y)
        'US10Y.GBOND',
        'US5Y.GBOND',
        'US3Y.GBOND',
        'US1Y.GBOND',
        
        -- German Bund yields (available: 10Y, 5Y, 3Y, 1Y)
        'DE10Y.GBOND',
        'DE5Y.GBOND',
        'DE3Y.GBOND',
        'DE1Y.GBOND',
        
        -- Other European sovereigns
        'IT10Y.GBOND',  -- Italy
        'ES10Y.GBOND',  -- Spain
        'FR10Y.GBOND',  -- France
        'NL10Y.GBOND',  -- Netherlands
        
        -- Emerging Markets (useful for risk-on/risk-off signals)
        'BR10Y.GBOND',  -- Brazil
        'MX10Y.GBOND',  -- Mexico
        'CN10Y.GBOND'   -- China
    )

),

with_changes as (

    select
        price_date,
        symbol,
        yield,
        
        -- Daily change in yield (bps)
        (yield - lag(yield) over (partition by symbol order by price_date)) * 100 as yield_change_1d_bps,
        
        -- Weekly change
        lag(yield, 5) over (partition by symbol order by price_date) as yield_lag_1w

    from bonds

),

final as (

    select
        price_date,
        symbol,
        yield,
        yield_change_1d_bps,
        (yield - yield_lag_1w) * 100 as yield_change_1w_bps

    from with_changes

)

select * from final
