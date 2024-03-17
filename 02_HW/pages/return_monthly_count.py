import pandas as pd
import streamlit as st

from hw2 import *
from user_definition import *


st.title('Year, Month and Count')

n = st.text_input('Number of data to display:')

if n:
    data = return_monthly_count(user=user, host=host, dbname=dbname, n=n)

else:
    data = return_monthly_count(user=user, host=host, dbname=dbname)

df = pd.DataFrame(
    data, columns=['Year', 'Jan', 'Feb', 'Mar', 'Apr', 'May',
                   'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
    dtype=pd.Int64Dtype())
st.bar_chart(df, x='Year')
st.table(df)
