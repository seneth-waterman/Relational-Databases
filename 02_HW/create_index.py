import time

import streamlit as st

from hw1 import *
from hw2 import *

from user_definition import *


def retrieve_time_val(input):
    print(input[-1][0])
    return float(input[-1][0].split("Execution Time: ")[1].split(" ")[0])


def return_time_measure():
    return_count_by_location_report_type_incident_description_time = 0
    for i in range(0, test_time):
        time_measure =\
            return_count_by_location_report_type_incident_description(user=user,
                                                                      host=host,
                                                                      dbname=dbname,
                                                                      year=year,
                                                                      explain=True)
        return_count_by_location_report_type_incident_description_time +=\
            retrieve_time_val(time_measure)
    return_count_by_location_report_type_incident_description_time /= test_time

    return return_count_by_location_report_type_incident_description_time


def calculate_index_improvement(**kargs):
    user = kargs['user']
    host = kargs['host']
    dbname = kargs['dbname']
    dir = kargs['dir']

    drop_tables(user, host, dbname)
    create_tables(user, host, dbname)
    copy_data(user, host, dbname, dir)
    before_time = return_time_measure()

    drop_tables(user, host, dbname)

    create_tables(user, host, dbname)
    copy_data(user, host, dbname, dir)
    create_index(**kargs)
    after_time = return_time_measure()

    return (before_time-after_time)/before_time*100


def main():
    if 'dir' not in st.session_state:
        st.session_state.dir = None

    folder_path = st.text_input('Absoulte path of the directory\
                                for input data:')
    if folder_path:
        st.write('You entered: ', folder_path)
        st.session_state.dir = folder_path

        improvement = calculate_index_improvement(user=user,
                                                  host=host,
                                                  dbname=dbname,
                                                  dir=st.session_state.dir)

        st.divider()
        st.write(
            f"Q4 Improvement: {improvement} %")
        st.divider()


if __name__ == '__main__':
    main()
