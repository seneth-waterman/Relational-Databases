import streamlit as st

from hw1 import *
from user_definition import *


st.title('Incidents with Substring')

input = st.text_input('Substring and Number of data to display\
                      (comma separated):')
strings = input.strip().split(',')

if input and len(strings) == 1:
    data = return_incident_with_incident_substring(
        user, host, dbname,
        strings[0].strip().lower())
    st.table(data)

elif input and len(strings) > 1:
    data = return_incident_with_incident_substring(
        user, host, dbname,
        strings[0].strip().lower(),
        strings[1].strip())
    st.table(data)
