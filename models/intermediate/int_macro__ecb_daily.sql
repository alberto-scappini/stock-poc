{{
    config(
        materialized='table',
        partition_by={
            'field': 'observation_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['series_key']
    )
}}

/*
    ECB macro series filtered to key European indicators.
    Includes EU HICP (inflation), Euribor rates, and other macro data.
*/

with parsed as (

    select
        -- TIME_PERIOD can be YYYY-MM or YYYY-MM-DD format
        case 
            when length(time_period) = 7 then parse_date('%Y-%m', time_period)
            when length(time_period) = 10 then parse_date('%Y-%m-%d', time_period)
            else null
        end as observation_date,
        key as series_key,
        cast(observation_value as float64) as value,
        title
    from {{ ref('stg_ecb_series_observations') }}
    where observation_value is not null

)

select
    observation_date,
    series_key,
    value,
    title,
    
    -- Friendly series names
    case
        when series_key = 'ICP.M.U2.N.000000.4.ANR' then 'CPI_EU_YOY'
        when series_key like 'FM.M.U2.EUR.RT.MM.EURIBOR3MD%' then 'EURIBOR_3M'
        when series_key like 'FM.M.U2.EUR.RT.MM.EURIBOR6MD%' then 'EURIBOR_6M'
        when series_key like 'FM.M.U2.EUR.RT.MM.EURIBOR1YD%' then 'EURIBOR_12M'
        when series_key like '%EONIA%' then 'EONIA'
        when series_key like '%ESTR%' or series_key like '%ESTER%' then 'ESTR'
        else series_key
    end as series_name

from parsed
where observation_date is not null
  and series_key in (
    -- EU HICP (CPI YoY)
    'ICP.M.U2.N.000000.4.ANR',
    
    -- Euribor rates (add actual keys from your data)
    'FM.M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA',
    'FM.M.U2.EUR.RT.MM.EURIBOR6MD_.HSTA',
    'FM.M.U2.EUR.RT.MM.EURIBOR1YD_.HSTA'
  )

