import psycopg


def select_all(func):
    """
    Q1. Complete the select_all() decorator, which 1) retrieve
    keyword arguments including user, host and dbname,
    2) executes a SQL query string returned
    from a function and 3) returns the output.
    Ex.
    When
    @select_all
    def return_incident_category_count(**kargs):
        # complete
    , calling
    return_incident_category_count(user='postgres',
                                   host='127.0.0.1',
                                   dbname='msds691_HW',
                                   n=5)
    returns [('Other Miscellaneous', 101), ('Larceny Theft', 90),
    ('Robbery', 72), ('Drug Offense', 60), ('Burglary', 52)]
    """
    def wrapper(**kwargs):
        user = kwargs['user']
        host = kwargs['host']
        dbname = kwargs['dbname']
        with psycopg.connect(f"user='{user}' \
                            host='{host}' \
                            dbname='{dbname}'") as conn:
            with conn.cursor() as curs:
                query = func(**kwargs)
                curs.execute(query)
                output = curs.fetchall()
                return output
    return wrapper


def check_query_args(**kargs):
    query = kargs['query']
    if 'explain' in kargs and kargs['explain'] is True:
        query = 'EXPLAIN ANALYZE VERBOSE ' + query
    if 'n' in kargs:
        query = query + f" LIMIT {kargs['n']}"
    return query


@select_all
def return_incident_category_count(**kargs):
    """
    Q2. Complete the return_incident_category_count() function.
    This function connects to the database using the parameters
    user, host, dbname, and n, and retrieves n records of
    incident_category along with their corresponding count
    from the incident_type table.
    The function only retrieves records
    where incident_category is not null and orders them
    by count in descending order.
    If there are rows with the same count,
    the function sorts them alphabetically by incident_category
    in ascending order.
    If the parameter n is not provided, the function returns all rows.
    """
    query = """
        SELECT incident_category, COUNT(incident_category)
        FROM incident_type
        WHERE incident_category IS NOT NULL
        GROUP BY incident_category
        ORDER BY COUNT(incident_category) DESC, incident_category ASC
            """
    return check_query_args(query=query, **kargs)


@select_all
def return_incident_count_by_category_subcategory(**kargs):
    """
    Q3. Complete the return_incident_count_by_category_subcategory() function.
    This function connects to the database
    using the provided user, host, dbname,
    count_limit, and n parameters.
    It returns n records of incident_category, incident_subcategory,
    and their count (occurrence) in the incident table
    where the count is greater than count_limit.
    The output is ordered by occurrence in descending order.
    If there are records with the same count value, they are ordered
    by incident_category alphabetically (ascending).
    """
    query = f"""
        SELECT incident_category, incident_subcategory, COUNT(*)
        FROM incident
        LEFT JOIN incident_type
        ON incident.incident_code = incident_type.incident_code
        GROUP BY incident_category, incident_subcategory
        HAVING COUNT(*) > {kargs['count_limit']}
        ORDER BY count DESC, incident_category ASC
            """
    return check_query_args(query=query, **kargs)


@select_all
def return_count_by_location_report_type_incident_description(**kargs):
    """
    Q4. Complete
    the return_count_by_location_report_type_incident_description() function.
    This function connects to the database
    using the given user, host, dbname, year,
    and n parameters, and returns an output of n rows (if n is given) or
    all rows of the following columns: year (extracted from incident_datetime),
    month (also extracted from incident_datetime), longitude, latitude,
    neighborhood, report_type_description, incident_description,
    and the corresponding count, which is ordered by count in descending order,
    and then by year, month, longitude, latitude, report_type_description,
    and incident_description in ascending order.
    """
    query = f"""
        SELECT EXTRACT(year from incident.incident_datetime)::INTEGER AS year,
            EXTRACT(month from incident.incident_datetime)::INTEGER AS month,
            incident.longitude,
            incident.latitude,
            neighborhood,
            report_type_description,
            incident_description,
            COUNT(*)
        FROM incident
        JOIN location
        ON incident.latitude = location.latitude AND
            incident.longitude = location.longitude
        JOIN incident_type
        ON incident.incident_code = incident_type.incident_code
        JOIN report_type
        ON report_type.report_type_code = incident.report_type_code
        WHERE EXTRACT(year from incident.incident_datetime) = {kargs['year']}
        GROUP BY EXTRACT(year from incident.incident_datetime),
                EXTRACT(month from incident.incident_datetime),
                incident.longitude,
                incident.latitude,
                neighborhood,
                report_type_description,
                incident_description
        ORDER BY count DESC, year, month, incident.longitude,
                incident.latitude, neighborhood, report_type_description,
                incident_description
            """
    return check_query_args(query=query, **kargs)


@select_all
def return_avg_interval_days_per_incident_code(**kargs):
    """
    Q5.
    Complete the return_avg_interval_days_per_incident_code() function.
    This function calculates the average number of days taken between
    incident_datetime and report_datetime for each incident_code.
    Using user, host, dbname, and n, this function connects to the database
    and returns n rows of incident_code, incident_description,
    and avg_interval_days,
    where avg_interval_days is the average difference between report_datetime
    and incident_datetime extracted as days.
    The output should be ordered by avg_interval_days in descending order.
    If there are multiple rows with the same avg_interval_days,
    order by incident_code in ascending order.
    If n is not given, it returns all the rows.
    """
    query = """
        SELECT inc.incident_code, incident_description,
                FLOOR(AVG(diff)) AS average_response
        FROM (SELECT EXTRACT(day from
            (report_datetime - incident_datetime)) AS diff,
                incident_code FROM incident) AS inc
        LEFT JOIN incident_type
        ON incident_type.incident_code = inc.incident_code
        GROUP BY inc.incident_code, incident_description
        ORDER BY average_response DESC
            """
    return check_query_args(query=query, **kargs)


@select_all
def return_monthly_count(**kargs):
    """
    Q6.
    Complete the `return_monthly_count()` function.
    This function returns the number of incidents in each month of each year.
    Using `user`, `host`, `dbname`, and `n`, this function connects to
    the database and returns `n` rows of `year`, `jan`, `feb`, `mar`, `apr`,
    `may`, `jun`, `jul`, `aug`, `sep`, `oct`, `nov` and `dec`,
    where each column includes the number of incidents
    for the corresponding year and month, ordered by year in ascending order.
    If `n` is not given, it returns all the rows.
    """
    query = """
    SELECT cohort.year,
    SUM(CASE WHEN EXTRACT(month from incident_datetime) = 1 THEN 1 END) AS JAN,
    SUM(CASE WHEN EXTRACT(month from incident_datetime) = 2 THEN 1 END) AS FEB,
    SUM(CASE WHEN EXTRACT(month from incident_datetime) = 3 THEN 1 END) AS MAR,
    SUM(CASE WHEN EXTRACT(month from incident_datetime) = 4 THEN 1 END) AS APR,
    SUM(CASE WHEN EXTRACT(month from incident_datetime) = 5 THEN 1 END) AS MAY,
    SUM(CASE WHEN EXTRACT(month from incident_datetime) = 6 THEN 1 END) AS JUN,
    SUM(CASE WHEN EXTRACT(month from incident_datetime) = 7 THEN 1 END) AS JUL,
    SUM(CASE WHEN EXTRACT(month from incident_datetime) = 8 THEN 1 END) AS AUG,
    SUM(CASE WHEN EXTRACT(month from incident_datetime) = 9 THEN 1 END) AS SEP,
    SUM(CASE WHEN EXTRACT(month from incident_datetime) = 10 THEN 1 END)
    AS OCT,
    SUM(CASE WHEN EXTRACT(month from incident_datetime) = 11 THEN 1 END)
    AS NOV,
    SUM(CASE WHEN EXTRACT(month from incident_datetime) = 12 THEN 1 END) AS DEC
    FROM
        (SELECT DISTINCT EXTRACT(year from incident_datetime) AS year
        FROM incident
        GROUP BY year) AS cohort
    JOIN incident
    ON EXTRACT(year from incident.incident_datetime) = cohort.year
    GROUP BY cohort.year
    ORDER BY cohort.year
    """
    return check_query_args(query=query, **kargs)


def create_index(**kargs):
    """
    Q7. Assuming that the query
    return_count_by_location_report_type_incident_description() (Q4)
    is the most frequently used query in your database,
    complete create_index() which creates indexes to improve its performance
    by at least 10%.
    For this question, you can assume that there will be no insertions or
    updates made to the database afterwards.
    Using streamlit, the create_index  will display the query improvement
    after you enter the absolute path of the data directory.
    """
    user = kargs['user']
    host = kargs['host']
    dbname = kargs['dbname']
    with psycopg.connect(f"user='{user}' \
                         host='{host}' \
                         dbname='{dbname}'") as conn:
        with conn.cursor() as curs:
            query = """
                CREATE INDEX incident_year_index
                ON incident (EXTRACT(year FROM incident.incident_datetime));
                CLUSTER incident USING incident_year_index;
                """
            curs.execute(query)
