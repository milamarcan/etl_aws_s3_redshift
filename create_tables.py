import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drops each table using queries from sql_queries file.

    Arguments:
        cur: Cursor object
        conn: Connection to the database

    Returns: None
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates each table using queries from sql_queries file.

    Arguments:
        cur: Cursor object
        conn: Connection to the database

    Returns: None
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Establishes connection to the database and gets cursor to it; 
    closes the connection.

    Arguments: None

    Returns: None
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
