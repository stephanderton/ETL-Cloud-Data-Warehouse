import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import pandas as pd
import json
import time
import mylib
from mylib import logger
import re
from sql_queries import count_table_queries


def load_staging_tables(cur, conn):
    """
    Load raw data to staging tables for Sparkify DB.
    
    Execute each query from the *copy_table_queries* list; each query
    loads a specific staging table in the data warehouse.
    (list defined in sql_queries.py)

    Parameters:
        cur (cursor object) : for executing PostgreSQL command in a db session
        conn (db session object) : connection to a database session

    Returns:
        none

    """
    logger.info('Load staging tables...')
        
    for query in copy_table_queries:
        # the table name is the 2nd word in the query string
        table = re.findall(r'\w+', query)[1]
        logger.info('load staging table [ {} ]...'.format(table))

        try:
            cur.execute(query)
            conn.commit()
            
        except psycopg2.Error as e: 
            logger.info('Error: Staging table [ {} ]'.format(table))
            print(e)
            print(query)


def insert_tables(cur, conn):
    """
    Pull data from staging tables into final analytics tables.
    
    Execute each query from the *insert_table_queries* list; each query
    loads a specific analytics table in the data warehouse.
    (list defined in sql_queries.py)

    Parameters:
        cur (cursor object) : for executing PostgreSQL command in a db session
        conn (db session object) : connection to a database session

    Returns:
        none

    """
    logger.info('Load final tables...')

    for query in insert_table_queries:
        # the table name is the 3rd word in the query string
        table = re.findall(r'\w+', query)[2]
        logger.info('insert to table [ {} ]'.format(table))

        try:
            cur.execute(query)
            conn.commit()

        except psycopg2.Error as e: 
            logger.info('Error: Inserting to table [ {} ]'.format(table))
            print(e)
            print(query)


def count_table_rows(cur, conn):
    """
    Count the rows in analytics tables.
    
    Execute each query from the *count_table_queries* list; each query
    counts a specific analytics table in the data warehouse.
    (list defined in sql_queries.py)

    Parameters:
        cur (cursor object) : for executing PostgreSQL command in a db session
        conn (db session object) : connection to a database session

    Returns:
        none

    """
    logger.info('Check table counts...')

    for query in count_table_queries:
        # the table name is the last word in the query string
        table = re.findall(r'\w+', query)[-1]
            
        try:
            cur.execute(query)
            conn.commit()

            # the query returns the row count
            result = cur.fetchall()
            rows = result[0][0]
            logger.info("table count [ {} ] :  {}".format(table, rows))

        except psycopg2.Error as e: 
            logger.info('Error :  Issue counting table [ {} ]'.format(table))
            print(e)
            print(query)


def disable_result_cache(cur, conn):
    """
    Disable the result cache for the session

    Parameters:
        cur (cursor object) : for executing PostgreSQL command in a db session
        conn (db session object) : connection to a database session

    Returns:
        none

    """
    try:
        cur.execute("SET enable_result_cache_for_session TO OFF;")
        conn.commit()
        logger.info('Disable result cache for session')

    except psycopg2.Error as e: 
        logger.info("Error :  disabling result cache for session")
        print(e)


def log_config_params(config):
    """
    Write the S3 configuration parameters to the logfile (logger).

    Parameters:
        config (config file object) : access to the cluster configuration settings

    Returns:
        none

    """
    LOG_DATA     = config['S3']['LOG_DATA']
    LOG_JSONPATH = config['S3']['LOG_JSONPATH']
    SONG_DATA    = config['S3']['SONG_DATA']

    logger.info('LOG_DATA:  {}'.format(LOG_DATA))
    logger.info('LOG_JSONPATH:  {}'.format(LOG_JSONPATH))
    logger.info('SONG_DATA:  {}'.format(SONG_DATA))


def main():
    """
    Build ETL Pipeline for Sparkify song play data.
    
    Instantiate a session to the Postgres database on the Redshift cluster, 
    and acquire a cursor object to process SQL queries.
    
    """
        
    logger.info('---[ Begin ETL ]---')
    mylib.log_timestamp()
    print("Logfile:  " + mylib.get_log_file_name())

    # read config parameters for database connection string
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    log_config_params(config)

    try:
        conn_string = "host={} dbname={} user={} password={} port={}"
        conn_string = conn_string.format(*config['CLUSTER'].values())
        conn = psycopg2.connect( conn_string )
        cur = conn.cursor()

        print(conn_string)
        logger.info('DB connection :  open')

    except Exception as e:
        logger.info("Error :  Could not make connection to the sparkify DB")
        print(e)

    disable_result_cache(cur, conn)

    # Load Staging tables and Final analytics tables
    print("Load Staging tables...")
    load_staging_tables(cur, conn)

    print("Insert into Final tables...")
    insert_tables(cur, conn)

    print('Check table counts...')
    count_table_rows(cur, conn)

    conn.close()
    logger.info('DB connection :  closed')


if __name__ == "__main__":
    main()
