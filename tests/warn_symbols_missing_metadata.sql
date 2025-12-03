/*
    WARNING TEST: Symbols in price data that are missing from dim_symbol (metadata).
    
    This test identifies symbols that have price data but no corresponding
    entry in dim_symbol. These symbols will still appear in fct_etf_features_daily
    but will have NULL values for metadata-derived fields (is_bond_etf, region, etc.)
    
    This is a WARNING (not a failure) because:
    - The fact table intentionally includes all price data
    - Missing metadata is acceptable but should be tracked
    - Users should be aware of data coverage gaps
    
    To fix: Add the missing symbols to eod_symbols_metadata source
    or update the symbol mapping in dim_symbol.
*/

{{
    config(
        severity='warn',
        warn_if='>0'
    )
}}

with price_symbols as (
    -- All unique symbols from price data
    select distinct symbol
    from {{ ref('stg_eod_prices') }}
),

dim_symbols as (
    -- All symbols with metadata
    select distinct symbol
    from {{ ref('dim_symbol') }}
),

missing_metadata as (
    select 
        p.symbol,
        'Symbol has price data but no metadata in dim_symbol' as warning_message
    from price_symbols p
    left join dim_symbols d on p.symbol = d.symbol
    where d.symbol is null
)

select * from missing_metadata
