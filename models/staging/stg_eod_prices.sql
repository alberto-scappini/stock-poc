with source as (

    select * from {{ source('raw', 'eod_prices') }}

),

staged as (

    select
        _airbyte_raw_id as price_id,
        symbol,
        cast(date as date) as price_date,
        open as open_price,
        high as high_price,
        low as low_price,
        close as close_price,
        adjusted_close,
        volume,
        _airbyte_extracted_at as extracted_at

    from source

)

select * from staged

