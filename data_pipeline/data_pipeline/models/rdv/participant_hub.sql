{{
    config(
    materialized = 'table',
    schema = 'rdv',
    )
}}

with src_stage_table as (
    select distinct
        initcap(trim(coalesce(
            nullif(raw_data ->> 'First Name', ''), 
            nullif(raw_data ->> 'Buyer first name', '')
        ))) as participant_first_name
        
        , initcap(trim(coalesce(
            nullif(raw_data ->> 'Last Name', ''), 
            nullif(raw_data ->> 'Buyer last name', '')
        ))) as participant_last_name
        
        , lower(trim(coalesce(
            nullif(raw_data ->> 'Email', ''), 
            nullif(raw_data ->> 'Buyer email', ''), 
            nullif(raw_data ->> 'Email Address', '')
        ))) as participant_email

        , initcap(trim(coalesce(
            nullif(raw_data ->> 'City', '')
        ))) as participant_location
            
    from {{ source('stage', 'event_data') }}

)

, base_data as (
	select * 
	from (
		select 
			participant_first_name 
			, participant_last_name 
			, participant_email 
			, participant_location
			, row_number() over (
				partition by participant_email 
				order by length(participant_first_name || ' ' ||  participant_last_name) desc 
			) as rn
				
		from src_stage_table
	) as t 
	where rn = 1

)

select
    {{ dbt_utils.generate_surrogate_key(['participant_email']) }} as participant_id
	, bd.participant_first_name 
	, bd.participant_last_name 
	, bd.participant_email
	, bd.participant_location

from base_data as bd
where participant_email is not null
