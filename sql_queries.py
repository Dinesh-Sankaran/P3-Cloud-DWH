"""
Filename: sql_queries.py
Version: 1.0.0
Short Description: The script contain the SQL statements such as create, drop, copy and insert 

"""
# Import all necessary packages
import configparser


# Read the config paramater and assign values to variables
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA= config.get("S3","LOG_DATA")
SONG_DATA= config.get("S3","SONG_DATA")
ARN= config.get("IAM_ROLE","ARN")

# DROP TABLES
# SQL Statement to drop the tables if exists 
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# SQL Statement to create staging table(staging_events) if not exists with right datatype and conditions.
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist        VARCHAR,
        auth          VARCHAR NOT NULL,
        firstname     VARCHAR,
        gender        CHAR(1),
        iteminsession INT,
        lastName      VARCHAR,
        length        FLOAT,
        level         VARCHAR NOT NULL,
        location      VARCHAR,
        method        VARCHAR NOT NULL,
        page          VARCHAR NOT NULL,
        registration  BIGINT,
        sessionid     INT,
        song          VARCHAR,
        status        INT NOT NULL,
        ts            BIGINT NOT NULL,
        useragent     VARCHAR,
        userid        INT 
    )       
""")

# SQL Statement to create staging table(staging_songs) if not exists with right datatype and conditions.
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs        INT NOT NULL,
        artist_id        VARCHAR NOT NULL,
        artist_latitude  VARCHAR,
        artist_longitude VARCHAR,
        artist_location  VARCHAR,
        artist_name      VARCHAR NOT NULL,
        song_id          VARCHAR NOT NULL,
        title            VARCHAR NOT NULL,
        duration         FLOAT NOT NULL,
        year             INT NOT NULL
        )    
""")

# SQL Statement to create fact table(songplays) if not exists with autoincrement column songplay_id and right datatype for other columns
# ALso used KEY distribution style and the key identified is song_id
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id  INT IDENTITY(1,1) sortkey, 
        start_time   BIGINT, 
        user_id      INT NOT NULL, 
        level        VARCHAR, 
        song_id      VARCHAR distkey, 
        artist_id    VARCHAR, 
        session_id   VARCHAR, 
        location     VARCHAR, 
        user_agent   VARCHAR
    )    
""")

# SQL Statement to create dim table(users) with right datatype and conditions.Also distibution style identified as ALL.
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id    INT Primary key sortkey, 
        first_name VARCHAR, 
        last_name  VARCHAR, 
        gender     CHAR(1), 
        level      VARCHAR
    )diststyle all;
""")

# SQL Statement to create dim tale(songs) with right datatype and conditions.Also distibution style identified as ALL.
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id   VARCHAR Primary key distkey sortkey, 
        title     VARCHAR NOT NULL, 
        artist_id VARCHAR NOT NULL, 
        year      INT, 
        duration  FLOAT
    );
""")

# SQL Statement to create dim tale(artists) with right datatype and conditions.Also distibution style identified as ALL
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR NOT NULL Primary key sortkey, 
        name      VARCHAR NOT NULL, 
        location  VARCHAR,
        latitude  VARCHAR, 
        longitude VARCHAR
    )diststyle all;
""")

# SQL Statement to create dim tale(time) with right datatype and conditions.Also distibution style identified as ALL
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time BIGINT Primary key sortkey, 
        hour       INT, 
        day        INT, 
        week       INT,
        month      INT, 
        year       INT,
        weekday    INT
    )diststyle all;
""")

# STAGING TABLES
#Copy data to staging table called staging_events from event dataset which is available in s3 public bucket
staging_events_copy = ("""COPY staging_events FROM {} 
credentials 'aws_iam_role={}'
JSON 's3://udacity-dend/log_json_path.json' region 'us-west-2';
""").format(LOG_DATA,ARN)

#Copy data to staging table called staging_songs from song dataset which is available in s3 public bucket
staging_songs_copy = ("""COPY staging_songs FROM {}
credentials 'aws_iam_role={}'
JSON 'auto' region 'us-west-2';
""").format(SONG_DATA,ARN)

# FINAL TABLES
#Insert data into fact table(songplays) from staging table(staging_events and staging_songs). Have filtered valid records using 'Nextsong'in page  
songplay_table_insert = ("""
                               INSERT INTO songplays(
                                start_time,
                                user_id,
                                level,
                                song_id,
                                artist_id,
                                session_id,
                                location,
                                user_agent
                            )select
                                Distinct e.ts,e.userid,e.level,s.song_id,s.artist_id,e.sessionid,e.location,e.useragent 
                            from
                                staging_events e 
                            join
                                staging_songs s on e.artist=s.artist_name 
                                and e.length=s.duration 
                                and e.song=s.title
                            where
                                e.page='NextSong'
""")

#Insert data into dim table(users) from staging table(staging_events). Handling duplicates by using distinct keyword and valid records by using not null of userid
user_table_insert = ("""
                        INSERT INTO users (
                            user_id,
                            first_name,
                            last_name,
                            gender,
                            level
                        )  SELECT
                            DISTINCT userid, firstname, lastname, gender, level  
                        FROM
                            staging_events 
                        WHERE userid IS NOT NULL;
""")

#Insert data into dim table(songs) from staging table(staging_songs). Handling duplicates by using distinct keyword and valid records by using not null of song_id
song_table_insert = ("""
                        INSERT INTO songs (
                            song_id,
                            title,
                            artist_id,
                            year,
                            duration 
                        ) SELECT
                            DISTINCT song_id, title, artist_id, year, duration  
                        FROM
                            staging_songs 
                        WHERE song_id IS NOT NULL;
""")

#Insert data into dim table(artists) from staging table(staging_songs). Handling duplicates by using distinct keyword and valid records by using not null of artist_id
artist_table_insert = ("""
                        INSERT INTO artists(
                            artist_id,
                            name,
                            location,
                            latitude,
                            longitude
                        )SELECT
                            DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
                        FROM
                            staging_songs 
                        WHERE artist_id IS NOT NULL;                       
""")

#Insert data into dim table(time) from staging table(staging_events). Handling duplicates by using distinct keyword and valid records by using not null of ts
time_table_insert = ("""
                        INSERT INTO time(
                            start_time,
                            hour,
                            day,
                            week,
                            month,
                            year,
                            weekday
                        ) SELECT                        
                            temp.ts, 
                            EXTRACT(hour FROM temp.start_time) as Hour,
                            EXTRACT(day FROM temp.start_time) as day,
                            EXTRACT(week FROM temp.start_time) as week,
                            EXTRACT(month FROM temp.start_time) as month,
                            EXTRACT(year FROM temp.start_time) as year,
                            EXTRACT(weekday FROM temp.start_time) as weekday
                            FROM
                                (SELECT
                                    DISTINCT ts, timestamp 'epoch' + ts/1000 * interval '1 second' as start_time  
                                FROM
                                    staging_events 
                                WHERE ts IS NOT NULL
                                ) temp;
""")

# Query list for create table
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

# Query list for drop table
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# Query list for copy table
copy_table_queries = [staging_events_copy, staging_songs_copy]

# Query list for insert table
insert_table_queries = [user_table_insert,song_table_insert,artist_table_insert,time_table_insert,songplay_table_insert]