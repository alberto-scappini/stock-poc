{{
    config(
        materialized='table'
    )
}}

/*
    Dimension table for ETF/Symbol universe with classification and descriptions.
    
    Contains:
    - Symbol identification (ticker, ISIN, name)
    - Classification (etf_group, is_bond_etf, region)
    - Detailed descriptions from seed file (investment objective, composition)
    - Asset class and sub-asset class categorization
    
    This is the master reference for all symbols in the analytics universe.
    
    Note: Symbol is constructed as CODE.EXCHANGE_SUFFIX to match eod_prices format.
*/

with symbols as (

    select
        code,
        isin,
        name,
        asset_type,
        country,
        currency,
        exchange,
        -- Construct full symbol with exchange suffix to match eod_prices format
        code || '.' || case
            when exchange in ('NYSE ARCA', 'NYSE', 'NASDAQ', 'AMEX', 'BATS') then 'US'
            when exchange = 'LSE' then 'LSE'
            when exchange = 'XETRA' then 'XETRA'
            when exchange = 'Euronext Paris' then 'PA'
            when exchange = 'Euronext Amsterdam' then 'AS'
            when exchange = 'SIX Swiss Exchange' then 'SW'
            when exchange = 'Milan' then 'MI'
            when exchange = 'Madrid' then 'MC'
            when exchange = 'Frankfurt' then 'F'
            when exchange = 'Tokyo' then 'TSE'
            when exchange = 'Hong Kong' then 'HK'
            when exchange = 'Toronto' then 'TO'
            when exchange = 'Australian' then 'AU'
            else upper(replace(exchange, ' ', ''))
        end as symbol
    from {{ ref('stg_eod_symbols_metadata') }}

),

-- ETF descriptions from seed file
descriptions as (

    select
        symbol,
        description,
        investment_objective,
        asset_class as seed_asset_class,
        sub_asset_class,
        duration_category,
        credit_quality,
        geographic_focus,
        benchmark_index
    from {{ ref('etf_descriptions') }}

),

classified as (

    select
        s.symbol,
        s.code,
        s.isin,
        s.name,
        s.asset_type,
        s.country,
        s.currency,
        s.exchange,
        
        -- Classification based on ETF name patterns (fallback if not in seed)
        case
            when s.asset_type = 'ETF' and (
                lower(s.name) like '%corporate%' or 
                lower(s.name) like '%corp%' or
                lower(s.name) like '%investment grade%'
            ) then 'IG_BOND'
            
            when s.asset_type = 'ETF' and (
                lower(s.name) like '%high yield%' or 
                lower(s.name) like '%high-yield%' or
                lower(s.name) like '%junk%'
            ) then 'HY_BOND'
            
            when s.asset_type = 'ETF' and (
                lower(s.name) like '%treasury%' or 
                lower(s.name) like '%government%' or
                lower(s.name) like '%gilt%' or
                lower(s.name) like '%bund%'
            ) then 'TREASURY'
            
            when s.asset_type = 'ETF' and (
                lower(s.name) like '%aggregate%' or 
                lower(s.name) like '%agg%' or
                lower(s.name) like '%total bond%'
            ) then 'AGG_BOND'
            
            when s.asset_type = 'ETF' and (
                lower(s.name) like '%bond%' or
                lower(s.name) like '%fixed income%' or
                lower(s.name) like '%debt%'
            ) then 'OTHER_BOND'
            
            when s.asset_type = 'ETF' then 'EQUITY'
            
            else 'OTHER'
        end as etf_group,
        
        -- Flag for bond-like instruments (useful for ML)
        case
            when s.asset_type = 'ETF' and (
                lower(s.name) like '%bond%' or
                lower(s.name) like '%treasury%' or
                lower(s.name) like '%corporate%' or
                lower(s.name) like '%high yield%' or
                lower(s.name) like '%aggregate%' or
                lower(s.name) like '%fixed income%' or
                lower(s.name) like '%debt%' or
                lower(s.name) like '%gilt%' or
                lower(s.name) like '%bund%'
            ) then true
            else false
        end as is_bond_etf,
        
        -- Region classification
        case
            when s.exchange in ('NYSE ARCA', 'NYSE', 'NASDAQ', 'AMEX', 'BATS') or s.country = 'USA' then 'US'
            when s.exchange = 'LSE' or s.country = 'UK' then 'UK'
            when s.exchange = 'XETRA' or s.country in ('Germany', 'DE') then 'EU'
            when s.country in ('France', 'Italy', 'Spain', 'Netherlands', 'Belgium') then 'EU'
            else 'OTHER'
        end as region,
        
        -- Description and classification from seed (if available)
        d.description,
        d.investment_objective,
        coalesce(d.seed_asset_class, 
            case 
                when s.asset_type = 'ETF' and (
                    lower(s.name) like '%bond%' or
                    lower(s.name) like '%treasury%' or
                    lower(s.name) like '%corporate%' or
                    lower(s.name) like '%aggregate%' or
                    lower(s.name) like '%fixed income%'
                ) then 'FIXED_INCOME'
                else 'EQUITY'
            end
        ) as asset_class,
        d.sub_asset_class,
        d.duration_category,
        d.credit_quality,
        d.geographic_focus,
        d.benchmark_index

    from symbols s
    left join descriptions d on s.symbol = d.symbol
    where s.asset_type = 'ETF'

)

select * from classified
