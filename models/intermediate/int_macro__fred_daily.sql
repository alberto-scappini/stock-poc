{{
    config(
        materialized='table',
        partition_by={
            'field': 'observation_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['series_id']
    )
}}

/*
    FRED macro series filtered to key indicators.
    Includes US CPI, policy rates (EFFR, SOFR), and other macro indicators.
    
    Note: Filters out invalid numeric values (e.g., '.') that FRED sometimes reports
    for missing data points.
*/

select
    observation_date,
    series_id,
    safe_cast(observation_value as float64) as value
from {{ ref('stg_fred_series_observations') }}
where series_id in (
    -- Inflation
    'CPIAUCSL',         -- CPI All Urban Consumers
    'CPILFESL',         -- Core CPI (less food & energy)
    'PCEPI',            -- PCE Price Index
    'PCEPILFE',         -- Core PCE
    
    -- Policy rates
    'EFFR',             -- Effective Federal Funds Rate
    'SOFR',             -- Secured Overnight Financing Rate
    'DFF',              -- Federal Funds Rate
    
    -- Yield curve
    'T10Y2Y',           -- 10Y-2Y Treasury Spread
    'T10Y3M',           -- 10Y-3M Treasury Spread
    
    -- Economic indicators
    'UNRATE',           -- Unemployment Rate
    'PAYEMS',           -- Total Nonfarm Payrolls
    'INDPRO',           -- Industrial Production
    'RSAFS',            -- Retail Sales
    
    -- Financial conditions
    'BAMLH0A0HYM2',     -- ICE BofA US High Yield Index OAS
    'BAMLC0A0CM',       -- ICE BofA US Corporate Index OAS
    'TEDRATE',          -- TED Spread
    'DTWEXBGS'          -- Trade Weighted US Dollar Index
)
  and observation_value is not null
  -- Filter out FRED's missing value indicator '.'
  and observation_value != '.'
  and safe_cast(observation_value as float64) is not null
