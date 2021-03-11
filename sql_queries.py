import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events_table 
                            (
                            artist          VARCHAR,
                            auth            VARCHAR,
                            first_name      VARCHAR,
                            gender          VARCHAR,
                            item_in_session INTEGER,
                            last_name       VARCHAR,
                            length          DECIMAL,
                            level           VARCHAR,
                            location        VARCHAR,
                            method          VARCHAR,
                            page            VARCHAR,
                            registration    DECIMAL,
                            session_id      INTEGER,
                            song            VARCHAR,
                            status          INTEGER,
                            ts              TIMESTAMP,
                            user_agent      VARCHAR,
                            user_id         INTEGER
                            );"""
                             )

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table 
                            (
                            num_songs        INTEGER,
                            artist_id        VARCHAR,
                            artist_latitude  DECIMAL,
                            artist_longitude DECIMAL,
                            artist_location  VARCHAR,
                            artist_name      VARCHAR,
                            song_id          VARCHAR,
                            title            VARCHAR,
                            duration         DECIMAL,
                            year             INTEGER
                            );"""
                             )

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table 
                            (
                            songplay_id    int IDENTITY(0,1) PRIMARY KEY,
                            start_time     timestamp NOT NULL,
                            user_id        int NOT NULL,
                            level          varchar,
                            song_id        varchar NOT NULL,
                            artist_id      varchar NOT NULL,
                            session_id     int,
                            location       varchar,
                            user_agent     varchar
                            );"""
                        )

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table
                        (
                        user_id            int PRIMARY KEY,
                        first_name         varchar,
                        last_name          varchar,
                        gender             varchar,
                        level              varchar
                        );"""
                    )

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table
                        (
                        song_id            varchar PRIMARY KEY,
                        title              varchar,
                        artist_id          varchar NOT NULL,
                        year               int,
                        duration           numeric
                        );"""
                    )

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table
                          (
                          artist_id        varchar PRIMARY KEY,
                          name             varchar,
                          location         varchar,
                          latitude         numeric NULL,
                          longitude        numeric NULL
                          );"""
                      )

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table
                        (
                        start_time        timestamp PRIMARY KEY,
                        hour              int,
                        day               int,
                        week              int,
                        month             int,
                        year              int,
                        weekday           int
                        );"""
                    )

# STAGING TABLES

staging_events_copy = ("""
                          copy staging_events_table
                          from {}
                          iam_role {} 
                          json {}
                          TIMEFORMAT AS 'epochmillisecs';
                        """).format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'] )

staging_songs_copy = ("""
                          copy staging_songs_table
                          from {}
                          iam_role {} 
                          json 'auto';
                        """).format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
                            INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT events.ts as start_time, events.user_id, events.level, songs.song_id, songs.artist_id, events.session_id, events.location, events.user_agent
                            FROM staging_events_table AS events
                            LEFT JOIN staging_songs_table AS songs ON events.song = songs.title AND events.artist = songs.artist_name AND events.length = songs.duration 
                            WHERE events.page = 'NextSong' AND songs.song_id IS NOT NULL AND songs.artist_id IS NOT NULL;
                        """)

user_table_insert = ("""
                            INSERT INTO user_table (user_id, first_name, last_name, gender, level)
                            SELECT DISTINCT events.user_id, events.first_name, events.last_name, events.gender, events.level
                            FROM staging_events_table AS events
                            WHERE events.page = 'NextSong';
                        """)


song_table_insert = ("""
                        INSERT INTO song_table (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs_table
                        WHERE artist_id IS NOT NULL;
                    """)

artist_table_insert = ("""
                            INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
                            SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                            FROM staging_songs_table;
                        """)

time_table_insert = ("""
                            INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
                            SELECT DISTINCT ts, EXTRACT(hour from ts), EXTRACT(day from ts), EXTRACT(week from ts), EXTRACT(month from ts), EXTRACT(year from ts), EXTRACT(weekday from ts)
                            FROM staging_events_table
                            WHERE page = 'NextSong';
                        """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
