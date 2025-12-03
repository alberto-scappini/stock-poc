with source as (

    select * from {{ source('raw', 'supported_tickers_all_assets') }}

),

staged as (

    select
        _airbyte_raw_id as ticker_id,
        Code as code,
        Isin as isin,
        Name as name,
        Type as asset_type,
        Country as country,
        Currency as currency,
        Exchange as exchange,
        _airbyte_extracted_at as extracted_at

    from source

)

select * from staged

