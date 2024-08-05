# Import Python packages
import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session

# Get the current credentials
session = get_active_session()

# Create references to the tables
monthly_cpi_table = session.table("wages_cpi.data.monthly_cpi_usa")
annual_wages_cpi_table = session.table("wages_cpi.data.annual_wages_cpi_usa")

# Fetch data from Snowflake tables
monthly_cpi_data = monthly_cpi_table.to_pandas()
annual_wages_cpi_data = annual_wages_cpi_table.to_pandas()

# Streamlit app
st.title("Trends for Consumer Price Index and Annual Wages in the USA")

# Monthly CPI Data chart
st.subheader("Average monthly CPI, June 2021 through April 2024")
line = alt.Chart(monthly_cpi_data).mark_line(color='#29B5E8').encode(
    x=alt.X('MONTH:T', title='Month'),
    y=alt.Y('AVG_CPI:Q', 
            scale=alt.Scale(domain=[monthly_cpi_data['AVG_CPI'].min(), monthly_cpi_data['AVG_CPI'].max()]),
            axis=alt.Axis(title='Average CPI'))
)
points = line.mark_point()
cpi_chart = (line + points).properties(
    width=800,
    height=400
)
st.altair_chart(cpi_chart, use_container_width=True)

# Annual Wages and CPI Data chart
st.subheader("Average annual Wages and CPI, 2012 through 2022")
base = alt.Chart(annual_wages_cpi_data).encode(x='YEAR:T')

annual_wages_line = base.mark_line(color='#29B5E8').encode(
    y=alt.Y('AVG_ANNUAL_WAGES:Q', 
            scale=alt.Scale(domain=[annual_wages_cpi_data['AVG_ANNUAL_WAGES'].min() * 0.95, annual_wages_cpi_data['AVG_ANNUAL_WAGES'].max()]), 
            axis=alt.Axis(title='Average Annual Wages (USD)', titleColor='#29B5E8'))
)
annual_wages_points = annual_wages_line.mark_point()

cpi_line = base.mark_line(color='#D45B90').encode(
    y=alt.Y('CPI:Q', 
            scale=alt.Scale(domain=[annual_wages_cpi_data['CPI'].min() * 0.95, annual_wages_cpi_data['CPI'].max()]), 
            axis=alt.Axis(title='CPI', titleColor='#D45B90'))
)
cpi_points = cpi_line.mark_point()

combined_chart = alt.layer(
    annual_wages_line + annual_wages_points,
    cpi_line + cpi_points
).resolve_scale(
    y='independent'
).properties(
    width=800,
    height=400
)

st.altair_chart(combined_chart, use_container_width=True)