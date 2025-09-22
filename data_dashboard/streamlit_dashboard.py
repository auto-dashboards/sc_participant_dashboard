import streamlit as st
import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import plotly.express as px
from geopy.geocoders import Nominatim
from geopy.distance import geodesic

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=st.secrets["DB_HOST"],
        port=st.secrets["DB_PORT"],
        dbname=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"],
		sslmode="require"
    )

def run_sql_query(query):
    conn = get_connection()
    try:
        df = pd.read_sql_query(sql.SQL(query).as_string(conn), conn)
        return df 
    finally: 
        conn.close()


query = """
with event_data as (
	select distinct
		event_name 
		, to_date(event_date, 'DD/MM/YYYY') as event_date, 
		, row_number() over(order by event_date::date desc) as event_date_order
		
	from rdv.event_hub
)

, city_details as (
    select 
        city 
        , latitude
        , longitude

    from rdv.city_details
)

, base_data as (
	select distinct
		ph.participant_first_name 
		, ph.participant_last_name 
		, ph.participant_email 
        , ph.participant_location
		, eh.event_name
		, to_date(eh.event_date, 'DD/MM/YYYY') as event_date
        , eh.event_city
		, ed.event_date_order

        , case 
            when cd1.latitude is null or cd1.longitude is null 
                or cd2.latitude is null or cd2.longitude is null 
            then null 
            else 6371 * 2 * ASIN(
                SQRT(
                    POWER(SIN(RADIANS(cd2.latitude - cd1.latitude) / 2), 2)
                    + COS(RADIANS(cd1.latitude)) * COS(RADIANS(cd2.latitude))
                    * POWER(SIN(RADIANS(cd2.longitude - cd1.longitude) / 2), 2)
                )
            ) end as participant_distance_travelled

	from rdv.participant_event_link as pel
	
	inner join rdv.participant_hub as ph 
		on pel.participant_id = ph.participant_id
	
	inner join rdv.event_hub as eh
		on pel.event_id = eh.event_id
		
	left join event_data as ed
		on eh.event_name = ed.event_name

    left join city_details as cd1
        on ph.participant_location = cd1.city 

    left join city_details as cd2
        on eh.event_city = cd2.city 
)

select 
	participant_email 
    , participant_location
    , avg(participant_distance_travelled) as participant_avg_distance_travelled
	, (ROUND((COUNT(DISTINCT event_name)::float / (SELECT COUNT(DISTINCT event_name)::float FROM event_data))::numeric, 2)) * 100 AS percentage_events_attended
	, min(event_date_order) as latest_event_attended_order
	, (select max(event_date::date) from event_data) - max(event_date::date) as days_from_latest_event
	
from base_data as bd
group by participant_email, participant_location

"""

df = run_sql_query(query)

# Calculcate median cut offs for quadrant chart 
attendance_cutoff = df['percentage_events_attended'].max() / 2
eventsaway_cutoff = 3

df['latest_event_attended_order_jitter'] = df['latest_event_attended_order'] + np.random.uniform(-0.1, 0.1, size=len(df))
df['percentage_events_attended_jitter'] = df['percentage_events_attended'] + np.random.uniform(-0.1, 0.1, size=len(df))

threshold = df['participant_avg_distance_travelled'].quantile(0.8)
df['participant_avg_distance_travelled_threshold'] = np.where(df['participant_avg_distance_travelled'] >= threshold, 'highlight', 'non-highlight')

fig = px.scatter(
    df, x='latest_event_attended_order_jitter', y='percentage_events_attended_jitter', 
    hover_data={
        'latest_event_attended_order_jitter':False,
        'percentage_events_attended_jitter':False,
        'percentage_events_attended':True,
        'percentage_events_attended':True,
        'participant_email':True, 
        'percentage_events_attended':True, 
        'latest_event_attended_order':True, 
        'participant_location':True,
    },
    color='participant_avg_distance_travelled_threshold',
    color_discrete_map = {'highlight': '#0d3b66', 'non-highlight': "#95b8df"},
    size_max=6,
)

fig.add_hline(y=attendance_cutoff, line_dash="dash", line_color="red")
fig.add_vline(x=eventsaway_cutoff, line_dash="dash", line_color="red")

fig.update_traces(textposition="top center", marker=dict(size=6, line=dict(width=0.05, color="darkred")))

fig.update_layout(
    title="Quadrant Analysis of Participant Attendance",
    xaxis_title="Number of Events Away (from latest)",
    yaxis_title="Attendance %",
    height=600, width=800, 
    showlegend=False,
)

fig.update_xaxes(range=[20, 0], showgrid=False)
fig.update_yaxes(showgrid=False)

st.plotly_chart(fig, use_container_width=True)


