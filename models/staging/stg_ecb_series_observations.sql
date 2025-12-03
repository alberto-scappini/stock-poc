with source as (

    select * from {{ source('raw', 'ecb_series_observations') }}

),

staged as (

    select
        _airbyte_raw_id as observation_id,
        series_key,
        TIME_PERIOD as time_period,
        OBS_VALUE as observation_value,
        OBS_STATUS as observation_status,
        FREQ as frequency,
        UNIT as unit,
        TITLE as title,
        TITLE_COMPL as title_complete,
        REF_AREA as reference_area,
        ADJUSTMENT as adjustment,
        ICP_ITEM as icp_item,
        ICP_SUFFIX as icp_suffix,
        DECIMALS as decimals,
        UNIT_MULT as unit_multiplier,
        UNIT_INDEX_BASE as unit_index_base,
        COLLECTION as collection,
        COMPILATION as compilation,
        COVERAGE as coverage,
        DATA_COMP as data_compilation,
        SOURCE_AGENCY as source_agency,
        COMPILING_ORG as compiling_org,
        DISS_ORG as dissemination_org,
        STS_INSTITUTION as sts_institution,
        DOM_SER_IDS as domestic_series_ids,
        PUBL_ECB as publication_ecb,
        PUBL_MU as publication_mu,
        PUBL_PUBLIC as publication_public,
        OBS_CONF as observation_confidentiality,
        OBS_COM as observation_comment,
        OBS_PRE_BREAK as observation_pre_break,
        BREAKS as breaks,
        TIME_FORMAT as time_format,
        KEY as key,
        _airbyte_extracted_at as extracted_at

    from source

)

select * from staged

