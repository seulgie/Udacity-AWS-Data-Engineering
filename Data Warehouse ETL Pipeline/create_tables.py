import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table listed in the drop_table_queries list.
    
    Args:
        cur: Cursor object to execute PostgreSQL queries.
        conn: Connection object to the PostgreSQL database.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table listed in the create_table_queries list.
    
    Args:
        cur: Cursor object to execute PostgreSQL queries.
        conn: Connection object to the PostgreSQL database.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads the configuration from the 'dwh.cfg' file.
    - Establishes connection to the Redshift cluster using the provided credentials.
    - Drops any existing tables, then creates the required tables.
    - Closes the connection to the database.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
