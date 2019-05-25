import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load raw Sparkify data to staging tables.
    
    Execute each query from the *copy_table_queries* list; each query
    loads a specific staging table in the data warehouse.
    (list defined in sql_queries.py)

    Parameters:
        cur (cursor object) : for executing PostgreSQL command in a db session
        conn (db session object) : connection to a database session

    Returns:
        none

    """
        
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: Issue staging table")
            print(query)
            print(e)


def insert_tables(cur, conn):
    """
    """
        
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: Issue inserting to table")
            print(query)
            print(e)


def main():
    """
    Build ETL Pipeline for Sparkify song play data.
    
    Instantiate a session to the Postgres database on the Redshift cluster, 
    and acquire a cursor object to process SQL queries.
    
    """
        
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn_string = "host={} dbname={} user={} password={} port={}"
        conn_string = conn_string.format(*config['CLUSTER'].values())
        conn = psycopg2.connect( conn_string )
        cur = conn.cursor()
        print(conn_string)

    except Exception as e:
        print("Error: Could not make connection to the sparkify DWH")
        print(e)

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
