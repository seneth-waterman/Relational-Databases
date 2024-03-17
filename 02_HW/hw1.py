import psycopg


def drop_tables(user, host, dbname):
    """
    This function connects to a database
    using user, host and dbname, and
    drops all the tables including
    report_type, incident_type, location and incident.
    This function should work regardless of
    the existence of the table without any errors.
    """
    with psycopg.connect(f"user='{user}' \
                         host='{host}' \
                         dbname='{dbname}'") as conn:
        with conn.cursor() as curs:
            drop_tables =\
                """
                DROP TABLE IF EXISTS
                report_type, incident_type, location, incident
                CASCADE;
                """
            curs.execute(drop_tables)
        conn.commit()


def create_tables(user, host, dbname):
    """
    For the given user, host, and dbname,
    this function creates 4 different tables including
    report_type, incident_type, location and incident.
    The schema of each table is the following.
    report_type:
    report_type_code - varying character (length of 2), not null
    report_type_description - varying character (length of 100), not null
    primary key - report_type_code

    incident_type:
    incident_code - integer, not null
    incident_category - varying character (length of 100), null
    incident_subcategory - varying character (length of 100), null
    incident_description - varying character (length of 200), null
    primary key - incident_code

    location:
    longitude - real, not null
    latitude - real, not null
    supervisor_district - real, null
    police_district - varying character (length of 100), not null
    neighborhood - varying character (length of 100), null
    prinmary key - longitude, latitude

    incident:
    id - integer, not null
    incident_datetime - timestamp, not null
    report_datetime -  timestamp, not null
    longitude - real, null
    latitude - real, null
    report_type_code - varying character (length of 2), not null
    incident_code - integer, not null
    primary key - id
    foreign key - report_type_code, incident_code and
                  (longitude, latitude) pair.
    """
    with psycopg.connect(f"user='{user}' \
                         host='{host}' \
                         dbname='{dbname}'") as conn:
        with conn.cursor() as curs:
            report_type =\
                """
                CREATE TABLE report_type
                (
                    report_type_code VARCHAR(2) NOT NULL,
                    report_type_description VARCHAR(100) NOT NULL,
                    PRIMARY KEY (report_type_code)
                )
                """
            incident_type =\
                """
                CREATE TABLE incident_type
                (
                    incident_code INTEGER NOT NULL,
                    incident_category VARCHAR(100) NULL,
                    incident_subcategory VARCHAR(100) NULL,
                    incident_description VARCHAR(200) NULL,
                    PRIMARY KEY (incident_code)
                );
                """
            location =\
                """
                CREATE TABLE location
                (
                    longitude REAL NOT NULL,
                    latitude REAL NOT NULL,
                    supervisor_district REAL NULL,
                    police_district VARCHAR(100) NOT NULL,
                    neighborhood VARCHAR(100) NULL,
                    PRIMARY KEY(longitude, latitude)
                );
                """
            incident =\
                """
                CREATE TABLE incident
                (
                    id INTEGER NOT NULL,
                    incident_datetime TIMESTAMP NOT NULL,
                    report_datetime TIMESTAMP NOT NULL,
                    longitude REAL NULL,
                    latitude REAL NULL,
                    report_type_code VARCHAR(2) NOT NULL,
                    incident_code INTEGER NOT NULL,
                    PRIMARY KEY (id),
                    FOREIGN KEY (report_type_code)
                    REFERENCES  report_type (report_type_code)
                    ON UPDATE CASCADE,
                    FOREIGN KEY (incident_code)
                    REFERENCES  incident_type (incident_code)
                    ON UPDATE CASCADE,
                    FOREIGN KEY (longitude, latitude)
                    REFERENCES  location (longitude, latitude)
                    ON UPDATE CASCADE
                );
                """
            curs.execute(report_type)
            curs.execute(incident_type)
            curs.execute(location)
            curs.execute(incident)
        conn.commit()


def copy_data(user, host, dbname, dir):
    """
    Using user, host, dbname, and dir,
    this function connects to the database and
    loads data to report_type, incident_type, location and incident
    from report_type.csv, incident_type.csv, location.csv and incident.csv
    located in dir.
    Note: each file includes a header.
    """
    with psycopg.connect(f"user='{user}' \
                         host='{host}' \
                         dbname='{dbname}'") as conn:
        with conn.cursor() as curs:
            report_type =\
                f"""
                COPY report_type
                FROM '{dir}/report_type.csv'
                DELIMITER ','
                CSV HEADER;
                """
            incident_type =\
                f"""
                COPY incident_type
                FROM '{dir}/incident_type.csv'
                DELIMITER ','
                CSV HEADER;
                """
            location =\
                f"""
                COPY location
                FROM '{dir}/location.csv'
                DELIMITER ','
                CSV HEADER;
                """
            incident =\
                f"""
                COPY incident
                FROM '{dir}/incident.csv'
                DELIMITER ','
                CSV HEADER;
                """
            curs.execute(report_type)
            curs.execute(incident_type)
            curs.execute(location)
            curs.execute(incident)
        conn.commit()


def return_distinct_neighborhood_police_district(user, host, dbname, n=None):
    """
    Using user, host, dbname, dir, and n,
    this function connects to the database and
    returns n unique rows of neighborhood and police_district
    in the location table,
    where neighborhood is not null.
    The returned output is ordered by neighborhood and police_district
    in ascending order.
    If n is not given, it returns all the rows.
    """
    with psycopg.connect(f"user='{user}' \
                         host='{host}' \
                         dbname='{dbname}'") as conn:
        with conn.cursor() as curs:
            query =\
                """
                SELECT DISTINCT neighborhood, police_district
                FROM location
                WHERE neighborhood IS NOT NULL
                ORDER BY neighborhood, police_district
                """
            if n is not None:
                query = query + f" LIMIT {n}"
            curs.execute(query)
            return curs.fetchall()


def return_distinct_time_taken(user, host, dbname, n=None):
    """
    Using user, host, dbname, dir, and n,
    this function connects to the database and
    returns n unique differences
    between report_datetime and incident_datetime in days
    in descending order.
    If n is not given, it returns all the rows.
    """
    with psycopg.connect(f"user='{user}' \
                         host='{host}' \
                         dbname='{dbname}'") as conn:
        with conn.cursor() as curs:
            query =\
                """
                SELECT DISTINCT
                TO_CHAR(report_datetime - incident_datetime,
                        'dd')::INTEGER AS time
                FROM incident
                ORDER BY time DESC
                """
            if n is not None:
                query = query + f" LIMIT {n}"
            curs.execute(query)
            return curs.fetchall()


def return_incident_with_incident_substring(user,
                                            host,
                                            dbname,
                                            substr,
                                            n=None):
    """
    Using user, host, dbname, dir, substr, and n,
    this function connects to the database and
    returns n unique id and incident_datetime in the incident table
    where its incident_code corresonds to the incident_description
    that includes substr, a substring ordered by id in ascending order.
    The search for the existence of the given substring
    should be case-insensitive.
    If n is not given, it returns all the rows.
    """
    with psycopg.connect(f"user='{user}' \
                         host='{host}' \
                         dbname='{dbname}'") as conn:
        with conn.cursor() as curs:
            query =\
                f"""
                SELECT DISTINCT id, incident_datetime
                FROM incident
                WHERE incident_code IN
                (SELECT DISTINCT incident_code
                FROM incident_type
                WHERE LOWER(incident_description) LIKE '%{substr.lower()}%')
                ORDER BY id
                """
            if n is not None:
                query = query + f" LIMIT {n}"
            curs.execute(query)
            return curs.fetchall()


def return_incident_desc_for_report_type_desc(user,
                                              host,
                                              dbname,
                                              desc,
                                              n=None):
    """
    Using user, host, dbname, dir, substr, and n,
    this function connects to the database and
    returns n unique incident description in the incident_type table
    where its incident_code corresponds to desc as
    report_type_description ordered by incident_description in ascending order.
    The search of the report_type_description
    should be case-insensitive.
    If n is not given, it returns all the rows.
    """
    with psycopg.connect(f"user='{user}' \
                         host='{host}' \
                         dbname='{dbname}'") as conn:
        with conn.cursor() as curs:
            query =\
                f"""
                SELECT DISTINCT incident_description
                FROM incident_type
                WHERE incident_code IN
                (
                    SELECT incident_code
                    FROM incident
                    WHERE report_type_code=
                    (SELECT report_type_code
                    FROM report_type
                    WHERE LOWER(report_type_description) = '{desc.lower()}')
                )
                ORDER BY incident_description
                """
            if n is not None:
                query = query + f" LIMIT {n}"
            curs.execute(query)
            return curs.fetchall()


def update_report_type(user, host, dbname, from_str, to_str):
    """
    Using user, host, dbname, dir, and n,
    this function connects to the database and
    updates report_type_code from from_str to to_str
    on the report_type table.
    """
    with psycopg.connect(f"user='{user}' \
                         host='{host}' \
                         dbname='{dbname}'") as conn:
        with conn.cursor() as curs:
            query =\
                f"""
                UPDATE report_type
                SET report_type_code = '{to_str}'
                WHERE report_type_code = '{from_str}';
                """
            curs.execute(query)
        conn.commit()


# def print_count_data(curs):
#     print(curs.rowcount)
#     print(curs.fetchall())


# def main():
#     with psycopg.connect("user='postgres' \
#                          host='localhost' \
#                          dbname='msds691_HW'") as conn:
#         with conn.cursor() as curs:
#             drop_tables(curs)
#             create_tables(curs)
#             copy_data(curs, dir)
#             distinct_neighborhood_police_district(curs)
#             print_count_data(curs)
#             distinct_time_taken(curs)
#             print_count_data(curs)
#             incident_substring_count(curs, "object", 5)
#             print_count_data(curs)
#             incident_desc_for_report_type_desc(curs, "Vehicle Supplement", 5)
#             print_count_data(curs)
#             conn.commit()
