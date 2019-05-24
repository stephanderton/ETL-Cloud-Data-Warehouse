import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


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
        
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: Dropping table")
            print(query)
            print(e)


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
        
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: Issue creating table")
            print(query)
            print(e)


def main():
    """
        Create clean tables in a new instance of the sparkify data warehouse.
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


    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()