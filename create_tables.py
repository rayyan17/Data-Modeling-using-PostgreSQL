"""Script for Creating Database and Tables"""

import configparser

import psycopg2

from sql_queries import create_table_queries, drop_table_queries


def create_database(db_config):
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """

    # connect to default database
    conn = psycopg2.connect("host={} dbname={} user={} password={}".format(db_config["HOST"],
                                                                           db_config["DEFAULT_DB_NAME"],
                                                                           db_config["DB_USER"],
                                                                           db_config["DB_PASSWORD"]))
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS {}".format(db_config["OUTPUT_DB_NAME"]))
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect("host={} dbname={} user={} password={}".format(db_config["HOST"],
                                                                           db_config["OUTPUT_DB_NAME"],
                                                                           db_config["DB_USER"],
                                                                           db_config["DB_PASSWORD"]))
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error Dropping Table using query: ", query)
            print(e)


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.

    - Establishes connection with the sparkify database and gets
    cursor to it.

    - Drops all the tables.

    - Creates all tables needed.

    - Finally, closes the connection.
    """
    config = configparser.ConfigParser()
    config.read('psql.cfg')

    cur, conn = create_database(config["DATABASE"])

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
