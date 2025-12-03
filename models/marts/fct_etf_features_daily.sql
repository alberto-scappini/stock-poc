

{{
    config(
        materialized='table',
        partition_by={
            'field': 'feature_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['symbol']
    )
}}
/*

    Final feature table for ETF/Bond excess return prediction.
    
    Grain: one row per (date, symbol)
    
    IMPORTANT: 
    - This table is driven by PRICES (left side), not dim_symbol
    - This ensures we capture all price data even if symbol metadata is missing
    - Symbol metadata is LEFT JOINed from dim_symbol
    - Join to dim_symbol separately for full metadata (name, description, etc.)
    
    Available data sources:
    - Indices: VIX, DXY, EURUSD, GSPC (S&P500), SX5E (EuroStoxx50)
    - Gov Bonds: US(10Y,5Y,3Y,1Y), DE(10Y,5Y,3Y,1Y), IT10Y, ES10Y, FR10Y, NL10Y
    - No UK bonds available, No US2Y/US30Y available
    
    Features include:
    - Price-based: returns, momentum, volatility
    - Yield-based: trailing dividend yields
    - Macro: CPI, policy rates, credit spreads
    - Bond curve: government yields and slopes
    - Risk: VIX, equity indices, FX
*/

-- Price features (this is the BASE - drives the fact table)
with prices as (

    select
        price_date,
        symbol,
        close_price,
        adjusted_close,
        volume,
        return_1d,
        return_1w,
        return_1m,
        return_3m,
        return_6m,
        return_12m,
        volatility_21d,
        volatility_63d
    from {{ ref('int_prices__etf_daily') }}

),

-- Symbol metadata (LEFT JOIN - not all symbols may have metadata)
symbols as (

    select
        symbol,
        is_bond_etf,
        region
    from {{ ref('dim_symbol') }}

),

-- Dividend yield features
yields as (

    select
        price_date,
        symbol,
        yield_12m_ttm,
        yield_6m_ann,
        yield_3m_ann
    from {{ ref('int_yield__etf_daily') }}

),

-- Government bond yields (pivoted) - only available symbols
gov_bonds as (

    select
        price_date,
        symbol,
        yield
    from {{ ref('int_yields__gov_bonds_daily') }}

),

gov_bonds_pivot as (

    select
        price_date,
        -- US Treasury yields (available: 10Y, 5Y, 3Y, 1Y)
        max(case when symbol = 'US10Y.GBOND' then yield end) as us_10y,
        max(case when symbol = 'US5Y.GBOND' then yield end) as us_5y,
        max(case when symbol = 'US3Y.GBOND' then yield end) as us_3y,
        max(case when symbol = 'US1Y.GBOND' then yield end) as us_1y,
        
        -- German Bund yields (available: 10Y, 5Y, 3Y, 1Y)
        max(case when symbol = 'DE10Y.GBOND' then yield end) as de_10y,
        max(case when symbol = 'DE5Y.GBOND' then yield end) as de_5y,
        max(case when symbol = 'DE3Y.GBOND' then yield end) as de_3y,
        max(case when symbol = 'DE1Y.GBOND' then yield end) as de_1y,
        
        -- European periphery (for spread calculations)
        max(case when symbol = 'IT10Y.GBOND' then yield end) as it_10y,
        max(case when symbol = 'ES10Y.GBOND' then yield end) as es_10y,
        max(case when symbol = 'FR10Y.GBOND' then yield end) as fr_10y,
        max(case when symbol = 'NL10Y.GBOND' then yield end) as nl_10y,
        
        -- Emerging markets (risk sentiment)
        max(case when symbol = 'BR10Y.GBOND' then yield end) as br_10y,
        max(case when symbol = 'MX10Y.GBOND' then yield end) as mx_10y

    from gov_bonds
    group by price_date

),

-- Index prices (pivoted) - only available symbols
indices as (

    select
        price_date,
        symbol,
        close_price,
        return_1d as idx_return_1d,
        change_1w as idx_change_1w
    from {{ ref('int_prices__indices_daily') }}

),

indices_pivot as (

    select
        price_date,
        -- Volatility index
        max(case when symbol = 'VIX.INDX' then close_price end) as vix,
        
        -- Equity indices
        max(case when symbol = 'GSPC.INDX' then close_price end) as sp500,
        max(case when symbol = 'SX5E.INDX' then close_price end) as eurostoxx50,
        
        -- Currency
        max(case when symbol = 'DXY.INDX' then close_price end) as dxy,
        max(case when symbol = 'EURUSD.FOREX' then close_price end) as eurusd,
        
        -- Index returns
        max(case when symbol = 'GSPC.INDX' then idx_return_1d end) as sp500_return_1d,
        max(case when symbol = 'SX5E.INDX' then idx_return_1d end) as eurostoxx50_return_1d,
        max(case when symbol = 'VIX.INDX' then idx_return_1d end) as vix_return_1d

    from indices
    group by price_date

),

-- US macro data (forward-filled to daily)
macro_us_cpi as (

    select
        observation_date,
        cpi_us_yoy
    from {{ ref('int_macro__cpi_us_yoy') }}

),

macro_us_rates as (

    select
        observation_date,
        effr,
        sofr,
        fed_funds_rate
    from {{ ref('int_macro__rates_us') }}

),

macro_credit as (

    select
        observation_date,
        us_10y3m_spread,
        ted_spread
    from {{ ref('int_macro__credit_spreads') }}

),

-- EU macro data
macro_eu_cpi as (

    select
        observation_date,
        cpi_eu_yoy
    from {{ ref('int_macro__cpi_eu_yoy') }}

),

macro_euribor as (

    select
        observation_date,
        euribor_3m,
        euribor_6m,
        euribor_12m
    from {{ ref('int_macro__euribor') }}

),

-- Forward-fill macro data to daily
macro_daily as (

    select
        p.price_date as feature_date,
        
        -- US CPI (monthly, forward-fill)
        last_value(cpi.cpi_us_yoy ignore nulls) over (
            order by p.price_date
            rows between unbounded preceding and current row
        ) as cpi_us_yoy,
        
        -- US rates (daily or forward-fill)
        last_value(rates.effr ignore nulls) over (
            order by p.price_date
            rows between unbounded preceding and current row
        ) as effr,
        last_value(rates.sofr ignore nulls) over (
            order by p.price_date
            rows between unbounded preceding and current row
        ) as sofr,
        
        -- EU CPI (monthly, forward-fill)
        last_value(eu_cpi.cpi_eu_yoy ignore nulls) over (
            order by p.price_date
            rows between unbounded preceding and current row
        ) as cpi_eu_yoy,
        
        -- Euribor (monthly, forward-fill)
        last_value(euribor.euribor_3m ignore nulls) over (
            order by p.price_date
            rows between unbounded preceding and current row
        ) as euribor_3m

    from (select distinct price_date from prices) p
    left join macro_us_cpi cpi on p.price_date = cpi.observation_date
    left join macro_us_rates rates on p.price_date = rates.observation_date
    left join macro_credit credit on p.price_date = credit.observation_date
    left join macro_eu_cpi eu_cpi on p.price_date = eu_cpi.observation_date
    left join macro_euribor euribor on p.price_date = euribor.observation_date

),

-- Final assembly - PRICES is the driver (left side)
final as (

    select
        -- Keys
        p.price_date as feature_date,
        p.symbol,
        
        -- Flag to indicate if symbol has metadata
        case when sym.symbol is not null then true else false end as has_symbol_metadata,
        
        -- Price features
        p.close_price,
        p.adjusted_close,
        p.volume,
        p.return_1d,
        p.return_1w,
        p.return_1m,
        p.return_3m,
        p.return_6m,
        p.return_12m,
        p.volatility_21d,
        p.volatility_63d,
        
        -- Dividend yield features
        y.yield_12m_ttm,
        y.yield_6m_ann,
        y.yield_3m_ann,
        
        -- US Government bond yields
        g.us_10y,
        g.us_5y,
        g.us_3y,
        g.us_1y,
        
        -- German Bund yields
        g.de_10y,
        g.de_5y,
        g.de_3y,
        g.de_1y,
        
        -- European periphery yields
        g.it_10y,
        g.es_10y,
        g.fr_10y,
        
        -- Emerging market yields (risk sentiment)
        g.br_10y,
        
        -- US Yield curve slopes (using available tenors)
        (g.us_10y - g.us_3y) as us_slope_10y3y,
        (g.us_10y - g.us_1y) as us_slope_10y1y,
        (g.us_5y - g.us_1y) as us_slope_5y1y,
        
        -- German yield curve slopes
        (g.de_10y - g.de_3y) as de_slope_10y3y,
        (g.de_10y - g.de_1y) as de_slope_10y1y,
        
        -- European sovereign spreads (vs Germany)
        (g.it_10y - g.de_10y) as italy_spread,
        (g.es_10y - g.de_10y) as spain_spread,
        (g.fr_10y - g.de_10y) as france_spread,
        
        -- EM spread vs US (risk sentiment)
        (g.br_10y - g.us_10y) as brazil_spread_vs_us,
        
        -- US-DE spread (transatlantic)
        (g.us_10y - g.de_10y) as us_de_spread,
        
        -- Risk indicators
        idx.vix,
        idx.dxy,
        idx.eurusd,
        idx.sp500,
        idx.eurostoxx50,
        idx.sp500_return_1d,
        idx.eurostoxx50_return_1d,
        idx.vix_return_1d,
        
        -- Macro features
        m.cpi_us_yoy,
        m.cpi_eu_yoy,
        m.effr,
        m.sofr,
        m.euribor_3m,
        
        -- Derived features for ML
        
        -- Yield spread vs benchmark (for bond ETFs)
        case 
            when sym.is_bond_etf and sym.region = 'US' then y.yield_12m_ttm - g.us_10y
            when sym.is_bond_etf and sym.region = 'EU' then y.yield_12m_ttm - g.de_10y
            else null
        end as yield_spread_vs_benchmark,
        
        -- Real yield proxy (yield - inflation)
        (g.us_10y - m.cpi_us_yoy) as us_real_yield_10y,
        (g.de_10y - m.cpi_eu_yoy) as de_real_yield_10y,
        
        -- Policy rate spread (fed funds vs benchmark yield)
        (m.effr - g.us_10y) as policy_vs_10y_spread,
        
        -- Risk-adjusted return (Sharpe proxy)
        safe_divide(p.return_3m, p.volatility_63d) as sharpe_3m_proxy,
        
        -- VIX regime indicator
        case
            when idx.vix < 15 then 'LOW'
            when idx.vix < 25 then 'NORMAL'
            when idx.vix < 35 then 'ELEVATED'
            else 'HIGH'
        end as vix_regime,
        
        -- US Curve regime (using 10Y-3Y since 2Y not available)
        case
            when (g.us_10y - g.us_3y) < 0 then 'INVERTED'
            when (g.us_10y - g.us_3y) < 0.25 then 'FLAT'
            when (g.us_10y - g.us_3y) < 0.75 then 'NORMAL'
            else 'STEEP'
        end as us_curve_regime

    from prices p
    -- LEFT JOIN - prices drive the table
    left join symbols sym on p.symbol = sym.symbol
    left join yields y on p.price_date = y.price_date and p.symbol = y.symbol
    left join gov_bonds_pivot g on p.price_date = g.price_date
    left join indices_pivot idx on p.price_date = idx.price_date
    left join macro_daily m on p.price_date = m.feature_date

)

select * from final
