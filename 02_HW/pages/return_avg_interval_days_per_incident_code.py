import pandas as pd
import streamlit as st

from hw2 import *
from user_definition import *


st.title('Incident Code, Description and Average Interval (Days)')

n = st.text_input('Number of data to display:')

if n:
    data = return_avg_interval_days_per_incident_code(
        user=user, host=host, dbname=dbname, n=n)

    df = pd.DataFrame(
        data, columns=['Incident Code',
                       'Description',
                       'Average Interval (Days)'])
    st.bar_chart(df, x='Description', y='Average Interval (Days)')
    st.table(df)


else:
    data = return_avg_interval_days_per_incident_code(
        user=user, host=host, dbname=dbname)
    df = pd.DataFrame(
        data, columns=['Incident Category',
                       'Description',
                       'Average Interval (Days)'])
    st.bar_chart(df, x='Description', y='Average Interval (Days)')
    st.table(df)
