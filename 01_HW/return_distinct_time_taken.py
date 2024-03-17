import streamlit as st

from hw1 import *
from user_definition import *


st.title('Distinct Time Taken')

n = st.text_input('Number of data to display:')

if n:
    data = return_distinct_time_taken(user, host, dbname, n)
    st.table(data)

else:
    data = return_distinct_time_taken(user, host, dbname)
    st.table(data)
