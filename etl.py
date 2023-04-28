import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Loads staging tables using queries from sql_queries file.

    Arguments:
        cur: Cursor object
        conn: Connection to the database

    Returns: None
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Inserts data into staging tables using queries from sql_queries file.

    Arguments:
        cur: Cursor object
        conn: Connection to the database

    Returns: None
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Establishes connection to the database and gets cursor to it;
    loads staging tables; inserts data into the tables; 
    closes the connection.

    Arguments: None

    Returns: None
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
