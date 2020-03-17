"""
Filename: create_tables.py
Version: 1.0.0
Short Description: The script connects to the Sparkify redshift database and perform drop and create tables 

"""
# Import all necessary packages and local files
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Function to execute drop table statement one by one 
def drop_tables(cur, conn):
    #Iterate each drop table query and execute
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        

# Function to execute create table statement one by one 
def create_tables(cur, conn):
    #Iterate each create table query and execute
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    #Read the config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
       
    #Establish the connection to redshift database by using defined config parameters
    print("Initiating DB connection...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # function call to drop_tables
    drop_tables(cur, conn)
    print("Tables Dropped Successfully")
    
    # function call to create_tables
    create_tables(cur, conn)
    print("Tables Created Successfully")

    conn.close()


if __name__ == "__main__":
    main()