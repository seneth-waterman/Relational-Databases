import pandas as pd
import streamlit as st

from hw2 import *
from user_definition import *


st.title('Incident Category, Subcategory and Count')

input = st.text_input(
    'Count limit and the number of data to display (comma separated):')
strings = input.strip().split(',')

if input and len(strings) == 1:
    count_limit = strings[0]
    data = return_incident_count_by_category_subcategory(
        user=user, host=host, dbname=dbname, count_limit=count_limit)

    df = pd.DataFrame(
        data, columns=['Incident Category', 'Incident Subcategory', 'Count'])
    st.bar_chart(df, x='Incident Subcategory', y='Count')
    st.table(df)


elif input and len(strings) > 1:
    count_limit, n = strings[0], strings[1]
    data = return_incident_count_by_category_subcategory(
        user=user, host=host, dbname=dbname, count_limit=count_limit, n=n)
    df = pd.DataFrame(
        data, columns=['Incident Category', 'Incident Subcategory', 'Count'])
    st.bar_chart(df, x='Incident Subcategory', y='Count')
    st.table(df)
