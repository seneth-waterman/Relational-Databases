import pandas as pd
import streamlit as st

from hw2 import *
from user_definition import *


st.title('Incident Year, Month, Location, Description and Count')

input = st.text_input(
    'Year and the number of data to display (comma separated):')
strings = input.strip().split(',')

if input and len(strings) == 1:
    year = strings[0]
    data = return_count_by_location_report_type_incident_description(
        user=user, host=host, dbname=dbname, year=year)

    df = pd.DataFrame(data, columns=['Year', 'Month', 'Longitude', 'Latitude',
                                     'Neighborhood', 'Report Type Description',
                                     'Incident Description', 'Count'])
    st.map(df, latitude='Latitude', longitude='Longitude', size='Count')
    st.table(df)


elif input and len(strings) > 1:
    year, n = strings[0], strings[1]
    data = return_count_by_location_report_type_incident_description(
        user=user, host=host, dbname=dbname, year=year, n=n)
    df = pd.DataFrame(
        data, columns=['Year', 'Month', 'Longitude', 'Latitude',
                       'Neighborhood', 'Report Type Description',
                       'Incident Description', 'Count'])
    st.map(df, latitude='Latitude', longitude='Longitude', size='Count')
    st.table(df)
