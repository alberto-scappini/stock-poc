with source as (

    select * from {{ source('raw', 'supported_crypto_currencies') }}

),

staged as (

    select
        _airbyte_raw_id as crypto_id,
        Code as code,
        Name as name,
        Type as asset_type,
        Country as country,
        Currency as currency,
        Exchange as exchange,
        _airbyte_extracted_at as extracted_at

    from source

)

select * from staged

