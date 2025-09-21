{{
    config(
    materialized = 'table',
    schema = 'rdv',
    )
}}

with src_stage_table as (
    select distinct
        coalesce(
            nullif(raw_data ->> 'Event name', ''), 
            nullif(raw_data ->> 'Event Name', '')
        ) as event_name
        , nullif(raw_data ->> 'Venue Name', '') as event_venue

    from {{ source('stage', 'event_data') }}
)

, event_hub_missing as (
    select
        event_name
        , event_date
        , event_city
    
    from {{ ref('event_hub_missing') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['sst.event_name']) }} as event_id
	, sst.event_name 
	, event_date 
	, event_venue
    , event_city

from src_stage_table as sst

left join event_hub_missing as ehm
    on sst.event_name = ehm.event_name

where sst.event_name is not null