import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays; "
user_table_drop = "DROP TABLE IF EXISTS users; "
song_table_drop = "DROP TABLE IF EXISTS songs; "
artist_table_drop = "DROP TABLE IF EXISTS artists; "
time_table_drop = "DROP TABLE IF EXISTS time; "


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR(500),
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    ItemInSession INT,
    lastName VARCHAR,
    length VARCHAR,
    level VARCHAR,
    location VARCHAR(500),
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userId INT)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    song_id VARCHAR PRIMARY KEY NOT NULL,
    artist_id VARCHAR,
    artist_latitude float,
    artist_longitude float,
    artist_location VARCHAR(500),
    artist_name VARCHAR(500),
    duration float,
    num_songs INT,
    title VARCHAR,
    year INT)
""")

songplay_table_create = (""" 
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(0,1) PRIMARY KEY, 
    start_time timestamp NOT NULL,
    user_id int NOT NULL, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location VARCHAR(500), 
    user_agent varchar);  
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY NOT NULL,
    first_name varchar NOT NULL,
    last_name varchar NOT NULL,
    gender varchar NOT NULL,
    level varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY NOT NULL,
    title varchar NOT NULL,
    artist_id varchar NOT NULL,
    year int NOT NULL,
    duration float NOT NULL);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    name VARCHAR(500) NOT NULL, 
    location VARCHAR(500), 
    latitude float, 
    longitude float);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour int, 
    day int, 
    week int, 
    month int, 
    year int,                      
    weekday int);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json {}
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH')) 

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto'
    """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN')) 

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time, e.userid, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
    FROM staging_events e 
    LEFT JOIN staging_songs s ON e.song = s.title AND e.length = s.duration
    WHERE e.page = 'NextSong'
""")

user_table_insert = """
INSERT INTO users(user_id,first_name,last_name,gender,level)
    SELECT userId, firstName, lastName, gender, level
    FROM staging_events
    where page = 'NextSong'
"""

song_table_insert = ("""
INSERT INTO songs(song_id,title,artist_id,year,duration)
    SELECT song_id,title, artist_id, year, duration
    FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id,name,location,latitude,longitude)
    SELECT artist_id,artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time(start_time,hour,day,week,month,year,weekday)
    SELECT ts ,EXTRACT(HOUR FROM ts ) hour_ts, EXTRACT(DAY FROM ts ) day_ts, EXTRACT(WEEK FROM ts ) week_ts,
            EXTRACT(MONTH FROM ts )month_ts, EXTRACT(YEAR FROM ts ) year_ts, EXTRACT(dayofweek FROM ts )dayofweek_ts  FROM (
                SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as ts
                FROM staging_events e 
                WHERE e.page = 'NextSong'
            )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]


# create separate schemas for staging, dimensions and facts and create each table in the appropriate schema.
# CREATE SCHEMAS
fact_schema= ("CREATE SCHEMA IF NOT EXISTS fact_tables")
dimension_schema= ("CREATE SCHEMA IF NOT EXISTS dimension_tables")
staging_schema= ("CREATE SCHEMA IF NOT EXISTS staging_tables")

# DROP SCHEMAS
fact_schema_drop= ("DROP SCHEMA IF EXISTS fact_tables CASCADE")
dimension_schema_drop= ("DROP SCHEMA IF EXISTS dimension_tables CASCADE")
staging_schema_drop= ("DROP SCHEMA IF EXISTS staging_tables CASCADE")

create_schemas_queries =[fact_schema,dimension_schema,staging_schema]
drop_schemas_queries= [fact_schema_drop,dimension_schema_drop,staging_schema_drop]