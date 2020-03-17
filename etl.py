"""
Filename: etl.py
Version: 1.0.0
Short Description: The script connects to the Sparkify redshift database, loads log_data and song_data into staging tables, and transforms them into the five tables  

"""

# Import all necessary packages and local files
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    The function to execute copy statement from S3 bucket to staging table
    
    Parameters:
        cur: cursor to perform database operation
        conn: connection to commit changes
    """
    for query in copy_table_queries:
        print("Copy statement started to execute.. Please wait..")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    The function to execute insert statement from staging tables to analytic tables
    
    Parameters:
        cur: cursor to perform database operation
        conn: connection to commit changes
    """
    for query in insert_table_queries:
        print("Insert statement started to execute.. Please wait..")
        cur.execute(query)
        conn.commit()


def main():
    #Read the config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #Establish the connection to redshift database by using defined config parameters
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # function call to load_staging_tables
    print("staging table load started..")
    load_staging_tables(cur, conn)
    
    # function call to insert_tables
    print("Copied!!")
    insert_tables(cur, conn)
    print("Inserted!!")
    conn.close()


if __name__ == "__main__":
    main()