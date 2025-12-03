with source as (

    select * from {{ source('raw', 'div_eod_div') }}

),

staged as (

    select
        _airbyte_raw_id as dividend_id,
        symbol,
        cast(date as date) as dividend_date,
        value as dividend_value,
        unadjustedValue as unadjusted_value,
        currency,
        period,
        cast(recordDate as date) as record_date,
        cast(paymentDate as date) as payment_date,
        cast(declarationDate as date) as declaration_date,
        _airbyte_extracted_at as extracted_at

    from source

)

select * from staged

