with source as (

    select * from {{ source('raw', 'id_mapping_metadata') }}

),

staged as (

    select
        _airbyte_raw_id as mapping_id,
        symbol,
        isin,
        cusip,
        figi,
        lei,
        cik,
        _airbyte_extracted_at as extracted_at

    from source

)

select * from staged

