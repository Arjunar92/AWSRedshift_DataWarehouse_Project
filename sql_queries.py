import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_song"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
time_table_drop = "DROP TABLE IF EXISTS dim_time"


# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                artist          varchar(MAX),
                                auth            varchar(MAX), 
                                firstName       varchar(MAX),
                                gender          char,   
                                itemInSession   int,
                                lastName        varchar(MAX),
                                length          float,
                                level           varchar, 
                                location        varchar,
                                method          varchar,
                                page            varchar,
                                registration    varchar,
                                sessionId       int,
                                song            varchar(MAX),
                                status          int,
                                ts              timestamp,
                                userAgent       varchar,
                                userId          int)""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                num_songs int,
                                artist_id varchar NOT NULL,
                                artist_latitude float, 
                                artist_longitude float, 
                                artist_location varchar, 
                                artist_name varchar(MAX), 
                                song_id varchar, 
                                title varchar(MAX), 
                                duration float,
                                year int)""")


######################################################################################################




songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                            songplay_id INT IDENTITY(1, 1) PRIMARY KEY,
                            start_time timestamp NOT NULL SORTKEY,
                            user_id int          NOT NULL,
                            level varchar(MAX),
                            artist_id varchar(MAX) NOT NULL DISTKEY,
                            song_id varchar(MAX) NOT NULL,
                            session_id int,
                            location text,
                            user_agent text)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                        user_id int              NOT NULL SORTKEY PRIMARY KEY,
                        first_name varchar(MAX)       NOT NULL,
                        last_name varchar(MAX)        NOT NULL,
                        gender char,
                        level varchar)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id varchar          NOT NULL SORTKEY PRIMARY KEY, 
                        title varchar(MAX),
                        artist_id varchar(MAX),
                        year int ,
                        duration float)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                        artist_id varchar(MAX)        NOT NULL SORTKEY PRIMARY KEY,
                        artist_name varchar(MAX), 
                        artist_location varchar(MAX), 
                        artist_latitude float,
                        artist_longitude float)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                        start_time timestamp     NOT NULL DISTKEY SORTKEY PRIMARY KEY,
                        hour int,
                        day int,
                        week int,
                        month int,
                        year int,
                        weekday int)""")

######################################################################################################
# STAGING TABLES

staging_events_copy = ("""
copy staging_events 
from {}
iam_role {}
json {}
COMPUPDATE OFF region 'us-west-2'
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
""").format(LOG_DATA, ARN,LOG_JSONPATH)
#COMPUPDATE OFF region 'us-west-2'

staging_songs_copy = ("""
copy staging_songs 
from {}
iam_role {}
json 'auto'
COMPUPDATE OFF region 'us-west-2'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
""").format(SONG_DATA,ARN)
#COMPUPDATE OFF region 'us-west-2'

#######################################################################################################
# FINAL TABLES

songplay_table_insert = ("""
                        INSERT INTO songplays                                                                               (start_time,user_id,level,artist_id,song_id,session_id,location,user_agent)
                        SELECT 
                                DISTINCT(se.ts), 
                                se.userId, 
                                se.level, 
                                ss.artist_id,
                                ss.song_id,
                                se.sessionId,
                                se.location,
                                se.userAgent
                        FROM staging_events as se 
                        INNER JOIN staging_songs as ss
                        ON se.artist = ss.artist_name AND se.song = ss.title AND se.length = ss.duration
                        WHERE se.page = 'NextSong'
                        
""")

user_table_insert = ("""
                        INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT  DISTINCT userId, 
                                firstName, 
                                lastName, 
                                gender, 
                                level 
                        FROM staging_events
                        WHERE userId IS NOT NULL AND page = 'NextSong';
""")

song_table_insert = ("""
                        INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id,
                               title,
                               artist_id,
                               year,
                               duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
                        INSERT INTO artists (artist_id,artist_name,artist_location,artist_latitude,artist_longitude)
                        SELECT DISTINCT artist_id,
                               artist_name,
                               artist_location,
                               artist_latitude,
                               artist_longitude
                        FROM   staging_songs
                        WHERE  artist_id IS NOT NULL
""")

time_table_insert = ("""
                        INSERT INTO time (start_time,hour,day,week,month,year,weekday)
                        SELECT  start_time,
                                EXTRACT(hour from start_time),
                                EXTRACT(day from start_time),
                                EXTRACT(week from start_time),
                                EXTRACT(month from start_time),
                                EXTRACT(year from start_time),
                                EXTRACT(weekday from start_time)
                        FROM songplays
                        
                         
""")

# ANALYTICAL QUERIES
get_number_staging_events = "SELECT COUNT(*) FROM staging_events"
get_number_staging_songs = "SELECT COUNT(*) FROM staging_songs"
get_number_songplays = "SELECT COUNT(*) FROM songplays"
get_number_users = "SELECT COUNT(*) FROM users"
get_number_songs = "SELECT COUNT(*) FROM songs"
get_number_artists = "SELECT COUNT(*) FROM artists"
get_number_time = "SELECT COUNT(*) FROM time"

# QUERY LISTS

create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, 
                        songplay_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create]

drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop, 
                      songplay_table_drop, 
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop]

copy_table_queries = [staging_events_copy, 
                      staging_songs_copy]

insert_table_queries = [songplay_table_insert, 
                        user_table_insert, 
                        song_table_insert, 
                        artist_table_insert, 
                        time_table_insert]


select_number_rows_queries = [
                            get_number_staging_events,
                            get_number_staging_songs,
                            get_number_songplays,
                            get_number_users,
                            get_number_songs,
                            get_number_artists,
                            get_number_time]