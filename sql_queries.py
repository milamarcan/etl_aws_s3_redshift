import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    ts timestamp,
    page varchar,
    userId varchar,
    firstName varchar,
    lastName varchar,
    gender varchar,
    level varchar,
    sessionId varchar,
    location varchar,
    userAgent text,
    artist varchar,
    auth text,
    itemInSession int,
    length float,
    method varchar,
    registration varchar,
    song varchar,
    status varchar
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id varchar,
    artist_name varchar(max),
    artist_location varchar(max),
    artist_latitude double precision,
    artist_longitude double precision,
    song_id varchar,
    title varchar(max),
    year int,
    duration double precision
);
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int identity(0,1) PRIMARY KEY, 
    start_time timestamp NOT NULL, 
    user_id int NOT NULL, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar(max), 
    user_agent varchar(max)
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY, 
    first_name varchar(max) NOT NULL, 
    last_name varchar(max) NOT NULL,  
    gender varchar, 
    level varchar
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar(max) NOT NULL, 
    artist_id varchar, 
    year int, 
    duration float NOT NULL
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    name varchar(max) NOT NULL, 
    location varchar(max), 
    latitude double precision, 
    longitude double precision
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY, 
    hour int NOT NULL, 
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL, 
    weekday int NOT NULL
);
"""

# STAGING TABLES

staging_events_copy = (
    """
        COPY staging_events FROM '{}'
        iam_role '{}'
        json '{}'
        REGION 'us-west-2';
    """
).format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))


staging_songs_copy = (
    """
        COPY staging_songs FROM '{}'
        iam_role '{}'
        REGION 'us-west-2';
    """
).format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT DISTINCT e.ts, e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
    
    FROM staging_events e
    JOIN staging_songs s
    ON (e.artist=s.artist_name AND e.song=s.title)
    WHERE e.page='NextSong'
"""

user_table_insert = """
INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events 
    WHERE page='NextSong'
"""

song_table_insert = """
INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
"""

artist_table_insert = """
INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
"""

time_table_insert = """
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts, EXTRACT(HOUR FROM ts), EXTRACT(DAY FROM ts), EXTRACT(WEEK FROM ts), EXTRACT(MONTH FROM ts), EXTRACT(YEAR FROM ts), EXTRACT(WEEKDAY FROM ts)
    FROM( 
       SELECT (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ') as ts
       FROM staging_events)
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
