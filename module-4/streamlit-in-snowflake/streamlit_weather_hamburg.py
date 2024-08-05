# Import Python packages
import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session
import pandas as pd
from snowflake.snowpark.functions import col

# Get the current credentials
session = get_active_session()

st.title('Weather and Sales Trends for Hamburg, Germany')

# Load the view and create a pandas dataframe 
hamburg_weather = session.table("WEATHER_HAMBURG").select(
    col("DATE"),
    col("DAILY_SALES"),
    col("AVG_TEMPERATURE_FAHRENHEIT"),
    col("AVG_PRECIPITATION_INCHES"),
    col("MAX_WIND_SPEED_100M_MPH")
).to_pandas()

hamburg_weather_long = hamburg_weather.melt('DATE', var_name='Measure', value_name='Value')

# Map column names to desired legend titles
hamburg_weather_long['Measure'] = hamburg_weather_long['Measure'].replace({
    'DAILY_SALES': 'Daily Sales ($)',
    'AVG_TEMPERATURE_FAHRENHEIT': 'Avg Temperature (°F)',
    'AVG_PRECIPITATION_INCHES': 'Avg Precipitation (in)',
    'MAX_WIND_SPEED_100M_MPH': 'Max Wind Speed (mph)'
})

# Create the Altair chart
chart = alt.Chart(hamburg_weather_long).mark_line(point=True).encode(
    x=alt.X('DATE:T', title='Date'),
    y=alt.Y('Value:Q', title='Values'),
    color=alt.Color('Measure:N', title='Legend', scale=alt.Scale(
        range=['#29B5E8', '#FF6F61', '#0072CE', '#FFC300']
    )),
    tooltip=['DATE:T', 'Measure:N', 'Value:Q']
).interactive().properties(
    width=700,
    height=400,
    title='Daily Sales, Temperature, Precipitation, and Wind Speed in Hamburg'
).configure_title(
    fontSize=20,
    font='Arial'
).configure_axis(
    grid=True
).configure_view(
    strokeWidth=0
)

# Display the chart in the Streamlit app
st.altair_chart(chart, use_container_width=True)