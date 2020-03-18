Summary:
========
A music streaming startup, Sparkify wants to build datawarehouse and ETL pipelines in AWS redshift database.

ER Diagram:
===========
![ER Diagram](https://raw.githubusercontent.com/Dinesh-Sankaran/P3-Cloud-DWH-S1/master/images/ER%20Diagram.png)

Source Files:
=============
This project use source as two json file which resides in AWS S3 bucket
 - Song Dataset
        Song Dataset contains a metadata about a song and artist of that songs
        
 - Log Dataset
        Log Dataset contains log details such as artist name,song title, auth mode,level,timestamps etc..

Staging Tables:
==============
We have used two staging table to store the data as it is from source json file.

**1.staging_events**
- Loads the data using copy statement from log dataset into staging table (staging_events) with right data types and conditions

**2.staging_songs**
- Loads the data using copy statement from song dataset into staging table (staging_songs) with right data types and conditions

Target Tables:
==============
We have identified 4 dimension and one fact tables to store data as like below.
while building DWH in cloud, selecting distribution key and sort key is  very important as it makes difference in data load and extraction.
#### Dimension
**1. users**
- Basically stores details of the user such as userid, firstname, lastname, etc..
- Primary key and Sort key is user_id
- Distribution Style: ALL

**2. songs**
- Basically stores details of the songs such as songid, title, artistid, duration of the song 
- Primary key, Sort key, dist key is song_id
- Distribution Style: KEY

**3. artists**
- artists table contain details of the artist. it can connect with songs table using artistid
- Primary key and Sort key is aritist_id
- Distribution Style: ALL
        
**4. time**
- time dimension is particularly developed for data analysis. Mostly users want to see their data by datetime group function
- Primary key and Sort key is start_time
- Distribution Style: ALL

#### Fact       
**songplays**
- songplays is a fact table which capture data from staging table (staging_songs) and get songid,artistid from staging_events where the record matches with song name,length and artist name
- It captures only valid record with page value equal to NextSong
- Sort key is songplay_id
- Distribution Style: song_id

Python Scripts:
===============
**1. create_tables.py**
###### Note: Before executing this script, the redshift cluster should be up and running
- This script contain two user defined functions - create tables and drop tables. it create the tables if not exists and drop the tables if exists
- It imports create table queries and drop table queries from sql_queries.py
- The main function establish the redshift database connection using config parameters specified in dwh.cfg and invoke create and drop table function 

**2. sql_queries.py**
- This script contain config parameter variables, sql queries for drop, create, insert and copy statement for staging table
- It pass create and drop table query as list to create_tables.py

**3. etl.py**
###### Note: Before executing this script, create_tables.py must be executed
This is the main script file which build etl flow for sparkify redshift database
- The method **load_staging_tables** execute copy statement from S3 bucket to staging table
- The method **insert_tables** execute the insert statement from staging tables in to analytic tables 

Execution Procedure:
====================
###### Note: 
Close all the open connection to sparkify database before running below scripts
1. Run the create_tables.py in command prompt
    ```sh
    python create_tables.py
    ```
    **Expected output:**
    Tables Dropped Successfully
    Tables Created Successfully

2. Run the etl.py 
    ```sh
    python etl.py
    ```
    **Expected output:**
    staging table load started..
    Copied!!
    Inserted!!
            
Validation Script:
==================
Go back to redshift query editor and execute below statements to ensure data is loaded properly into tables 
1. select count(*) from songplays
2. select count(*) from users
3. select count(*) from songs
4. select count(*) from artists
5. select count(*) from time

Data Quality by using distinct record count. It should match with about query record count
1. select distinct count(*) from songplays
2. select distinct count(*) from users
3. select distinct count(*) from songs
4. select distinct count(*) from artists
5. select distinct count(*) from time
    