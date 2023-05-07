import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist varchar,
    auth text, 
    firstName varchar,
    gender varchar,   
    itemInSession int,
    lastName varchar,
    length float,
    level varchar, 
    location varchar,
    method varchar,
    page varchar,
    registration bigint,
    sessionId int,
    song varchar,
    status int,
    ts timestamp,
    userAgent varchar,
    userId int);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    song_id varchar,
    num_songs int,
    title varchar,
    artist_name varchar,
    artist_latitude float,
    year int,
    duration float,
    artist_id varchar,
    artist_longitude float,
    artist_location varchar);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id int identity(0,1) primary key sortkey,
    start_time timestamp distkey,
    user_id int,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id int PRIMARY KEY sortkey,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id varchar PRIMARY KEY sortkey,
    title varchar, 
    artist_id varchar,
    year int,
    duration float);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id varchar PRIMARY KEY sortkey,
    name varchar, 
    location varchar, 
    latitude numeric,
    longitude numeric);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time timestamp PRIMARY KEY sortkey distkey,
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
    IAM_ROLE '{}'
    REGION '{}'
    FORMAT  JSON {}
    TIMEFORMAT  'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['CLUSTER']['REGION'],
    config['S3']['LOG_JSONPATH']
)

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role '{}'
    region '{}'
    json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'], config['CLUSTER']['REGION'])


# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    (SELECT DISTINCT 
    e.ts,
    e.userId,
    e.level,
    s.song_id,
    CAST(s.artist_id AS INT),
    e.sessionId,
    e.location,
    e.userAgent 
    FROM staging_events e
        LEFT JOIN staging_songs s
        ON e.song = s.title AND e.artist = s.artist_name and e.length = s.duration
    WHERE e.page = 'NextSong');
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)(
    SELECT DISTINCT userId,
    firstName,
    lastName,
    gender,
    level
    FROM staging_events
    WHERE userId IS NOT NULL
    AND page = 'NextSong');
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)(
    SELECT DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
    FROM staging_songs
    WHERE song_id IS NOT NULL);
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)(
    SELECT DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL);
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)(
    SELECT DISTINCT ts,
    EXTRACT(hour FROM ts),
    EXTRACT(day FROM ts),
    EXTRACT(week FROM ts),
    EXTRACT(month FROM ts),
    EXTRACT(year FROM ts),
    EXTRACT(dayofweek FROM ts)
    FROM staging_events
    WHERE ts IS NOT NULL);
""")

# QUERY LISTS
create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
