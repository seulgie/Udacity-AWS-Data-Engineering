import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 into the Redshift staging tables using the COPY command.
    
    Args:
        cur: Cursor object to execute PostgreSQL queries.
        conn: Connection object to the PostgreSQL database.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from the staging tables into the analytics (fact and dimension) tables.
    
    Args:
        cur: Cursor object to execute PostgreSQL queries.
        conn: Connection object to the PostgreSQL database.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads the configuration from the 'dwh.cfg' file.
    - Establishes connection to the Redshift cluster using the provided credentials.
    - Loads data into staging tables from S3.
    - Inserts data from staging tables into the analytics tables.
    - Closes the connection to the database.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
