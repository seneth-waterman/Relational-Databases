import psycopg


# pylint: disable=E1129
def select_all(func):
    def execute(**kargs):
        user = kargs["user"]
        host = kargs["host"]
        dbname = kargs["dbname"]
        with psycopg.connect(f"user='{user}' \
                            host='{host}' \
                            dbname='{dbname}'") as conn:
            with conn.cursor() as curs:
                curs.execute(func(**kargs))
                return curs.fetchall()
    return execute


def check_query_args(**kargs):
    query = kargs['query']
    if 'explain' in kargs and kargs['explain'] is True:
        query = 'EXPLAIN ANALYZE VERBOSE ' + query
    if 'n' in kargs:
        query = query + f" LIMIT {kargs['n']}"
    return query


def commit(func):
    """
    Q1. Complete the `commit()` decorator.
    This decorator should perform the following steps:
    a. Retrieve keyword arguments including
       `user`, `host`, `dbname`, and `isolation_level`.
    b. Create a connection using the `user`, `host`,
       and `dbname`, and set the isolation level.
    c. Execute a SQL query string returned from a function.
    d. Commit the changes.
    """
    def execute(**kargs):
        user = kargs["user"]
        host = kargs["host"]
        dbname = kargs["dbname"]
        isolation_level = kargs["isolation_level"]
        isolation_level_dic = {psycopg.IsolationLevel.READ_UNCOMMITTED: 1,
                               psycopg.IsolationLevel.READ_COMMITTED: 2,
                               psycopg.IsolationLevel.REPEATABLE_READ: 3,
                               psycopg.IsolationLevel.SERIALIZABLE: 4}
        with psycopg.connect(f"user='{user}' \
                            host='{host}' \
                            dbname='{dbname}'") as conn:
            conn._set_isolation_level(isolation_level_dic[isolation_level])
            with conn.cursor() as curs:
                curs.execute(func(**kargs))
                conn.commit()
                return None
    return execute


@commit
def create_view_incident_with_details(**kargs):
    """
    Q2. Create a view called incident_with_details, that includes id,
    incident_datetime, incident_code, incident_category,
    incident_subcategory, incident_description, longitude,
    latitude, report_datetime, report_type_code, report_type_description,
    supervisor_district, police_district and neighborhood
    for all the rows in incident table.
    """
    query = '''
        CREATE VIEW incident_with_details AS
        SELECT id, incident_datetime, i.incident_code, incident_category,
        incident_subcategory, incident_description, i.longitude, i.latitude,
        report_datetime, i.report_type_code, report_type_description,
        supervisor_district, police_district, neighborhood
        FROM incident as i
        LEFT JOIN incident_type as it
        ON i.incident_code = it.incident_code
        LEFT JOIN report_type as r
        ON i.report_type_code = r.report_type_code
        LEFT JOIN location as l
        ON l.latitude = i.latitude and l.longitude = i.longitude
        '''
    return check_query_args(query=query, **kargs)


@select_all
def daily_average_incident_increase(**kargs):
    """
    Q3. Complete the daily_average_incident_increase() function.
    This function connects to a database using the parameters
    user, host, dbname, and n. It returns n records of date
    and average_incident_increase.
    The date represents the date of incident_datetime,
    and average_incident_increase indicates
    the difference between the average number of incidents
    in the previous 6 days and the current date, and
    the average number of incidents between the current date
    and the next 6 days.
    The value is rounded to 2 decimal points (as float) and
    the records are ordered by date.
    If the parameter n is not provided, the function returns all rows.
    """
    query = '''
        SELECT date, round(avg_prev - avg_next, 2)::float as diff
        FROM(
        SELECT date(incident_datetime) as date, COUNT(*) as count,
        AVG(COUNT(*)) OVER (ORDER BY date(incident_datetime)
        RANGE BETWEEN CURRENT ROW AND INTERVAL '6 days' FOLLOWING) as avg_next,
        AVG(COUNT(*)) OVER (ORDER BY date(incident_datetime)
        RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW) as avg_prev
        FROM incident
        GROUP BY date(incident_datetime)
        ) as avgs
        '''
    return check_query_args(query=query, **kargs)


@select_all
def three_day_daily_report_type_ct(**kargs):
    """
    Q4. Complete the three_day_daily_report_type_ct() function.
    This function connects to a database using the parameters user,
    host, dbname, and n.
    It returns n records for all the incidents that occurred
    in the provided year and month.
    Each record includes the report_type_description,
    date, the number of incidents with the corresponding
    report_type_description one day before,
    the number of incidents with the corresponding
    report_type_description on the date,
    and the number of incidents with the corresponding
    report_type_description one day after.
    If the parameter n is not provided, the function returns all rows.
    """
    year_month = f"{kargs['year']}-{kargs['month']:02d}"
    query = f'''
        SELECT rtype.report_type_description, date,
            lag(numb_inc, 1) OVER (ORDER BY date) as nextday,
            numb_inc,
            lag(numb_inc, -1) OVER (ORDER BY date) as prevday
        FROM(
        SELECT TO_CHAR(date(i.incident_datetime), 'YYYY-MM') AS year_month,
            date(i.incident_datetime), report_type_description,
            COUNT(*) as numb_inc
        FROM incident as i
        JOIN report_type as r
        ON i.report_type_code = r.report_type_code
        GROUP BY date(i.incident_datetime), r.report_type_description
        HAVING report_type_description = 'Initial'
        ORDER BY date) as rtype
        WHERE year_month = '{year_month}'
        '''
    return check_query_args(query=query, **kargs)
