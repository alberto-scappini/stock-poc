with source as (

    select * from {{ source('raw', 'fred_series_observations') }}

),

staged as (

    select
        _airbyte_raw_id as observation_id,
        series_id,
        cast(date as date) as observation_date,
        value as observation_value,
        cast(realtime_start as date) as realtime_start,
        cast(realtime_end as date) as realtime_end,
        _airbyte_extracted_at as extracted_at

    from source

)

select * from staged

