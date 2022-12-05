import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries,drop_table_queries



def load_staging_tables(cur, conn):
    """ this function loops through the queries that load the data from the S3 bucket"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ this function loops through the queries that inserts the data from the staging
        tables into the analytical database table"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ this function sets the environment variables and run the whole script"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('1')
    load_staging_tables(cur, conn)
    print('2')
    insert_tables(cur, conn)
    print('3')

    conn.close()


if __name__ == "__main__":


    main()