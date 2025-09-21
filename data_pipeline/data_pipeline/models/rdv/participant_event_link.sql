{{
    config(
    materialized = 'table',
    schema = 'rdv',
    )
}}

with src_stage_table as (
    select distinct
        lower(trim(coalesce(
            nullif(raw_data ->> 'Email', ''), 
            nullif(raw_data ->> 'Buyer email', ''), 
            nullif(raw_data ->> 'Email Address', '')
        ))) as participant_email

        , coalesce(
            nullif(raw_data ->> 'Event name', ''), 
            nullif(raw_data ->> 'Event Name', '')
        ) as event_name

    from {{ source('stage', 'event_data') }}
)

, event_hub as (
    select distinct
        event_id
        , event_name 
    
    from {{ ref('event_hub') }}
)

, participant_hub as (
    select distinct
        participant_id
        , participant_email 
    
    from {{ ref('participant_hub') }}
)

select 
    {{ dbt_utils.generate_surrogate_key(['event_id', 'participant_id']) }} as participant_event_id
    , eh.event_id
    , ph.participant_id

from src_stage_table as sst

inner join event_hub as eh
    on sst.event_name = eh.event_name

inner join participant_hub as ph 
    on sst.participant_email = ph.participant_email
