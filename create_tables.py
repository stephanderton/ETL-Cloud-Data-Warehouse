import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import time
import re
import mylib
from mylib import logger


def drop_tables(cur, conn):
    """
    Drop all tables from the sparkify database.
    
    Execute each query from the *drop_table_queries* list; each query
    deletes a specific table from the Sparkify database.

    Parameters:
        cur (cursor object) : for executing PostgreSQL command in a db session
        conn (db session object) : connection to a database session

    Returns:
        none
     
    """
    logger.info('Drop existing tables...')

    for query in drop_table_queries:
        # the table name is the 5th word in the query string
        table = re.findall(r'\w+', query)[4]
        logger.info('delete table [ {} ]'.format(table))

        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            logger.info('Error: Dropping table [ {} ]'.format(table))
            print(e)
            print(query)


def create_tables(cur, conn):
    """
    Create the tables for a clean sparkify database.
    
    Execute each query from the *create_table_queries* list; each query
    creates a specific table in the Sparkify database.
    (list defined in sql_queries.py)

    Parameters:
        cur (cursor object) : for executing PostgreSQL command in a db session
        conn (db session object) : connection to a database session

    Returns:
        none

    """
    logger.info('Create tables...')

    for query in create_table_queries:
        # the table name is the 6th word in the query string
        table = re.findall(r'\w+', query)[5]
        logger.info('create table [ {} ]'.format(table))

        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            logger.info('Error: Creating table [ {} ]'.format(table))
            print(e)
            print(query)


def main():
    """
    Create clean tables in a new instance of the sparkify data warehouse.

    Instantiate a session to the Postgres database on the Redshift cluster, 
    and acquire a cursor object to process SQL queries.

    """
    
    logger.info('---[ Create Tables ]---')
    mylib.log_timestamp()
    print("Logfile:  " + mylib.log_file_name())

    # read config parameters for database connection string
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn_string = "host={} dbname={} user={} password={} port={}"
        conn_string = conn_string.format(*config['CLUSTER'].values())
        conn = psycopg2.connect( conn_string )
        cur = conn.cursor()
        print(conn_string)

    except Exception as e:
        logger.info("Error: Could not make connection to the sparkify DB")
        print(e)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    logger.info('DB connection closed')


if __name__ == "__main__":
    main()
