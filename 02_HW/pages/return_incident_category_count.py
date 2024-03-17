import pandas as pd
import streamlit as st

from hw2 import *
from user_definition import *


st.title('Incident Category and Count')

n = st.text_input('Number of data to display:')

if n:
    data = return_incident_category_count(user=user,
                                          host=host,
                                          dbname=dbname,
                                          n=n)
    df = pd.DataFrame(data, columns=['Incident Category', 'Count'])
    st.bar_chart(df, x='Incident Category', y='Count')
    st.table(df)


else:
    data = return_incident_category_count(user=user, host=host, dbname=dbname)
    df = pd.DataFrame(data, columns=['Incident Category', 'Count'])
    st.bar_chart(df, x='Incident Category', y='Count')
    st.table(df)
