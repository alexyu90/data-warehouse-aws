import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (artist varchar, auth varchar, firstName varchar, gender varchar, itemInSession int, \
                                 lastName varchar, length decimal, level varchar, location varchar, method varchar, page varchar, registration decimal, \
                                 sessionId int, song varchar, status int, ts timestamp, userAgent varchar, userId int);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (num_songs int, artist_id varchar, artist_latitude decimal, artist_longitude decimal, \
                                 artist_location varchar, artist_name varchar, song_id varchar, title varchar, duration decimal, year int);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id INT IDENTITY(0,1) PRIMARY KEY NOT NULL, start_time timestamp, user_id int NOT NULL, \
                            level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY NOT NULL, first_name varchar, last_name varchar, gender varchar, level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY NOT NULL, title varchar, artist_id varchar, year int, duration decimal);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY NOT NULL, name varchar, location varchar, \
                          latitude decimal(9,6), longitude decimal(9,6)); 
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, hour int, day int, week int, month int, year int, weekday int)
""")

# STAGING TABLES

# STAGING TABLES
staging_events_copy = ("""copy staging_events from {}
                          credentials 'aws_iam_role={}'
                          region 'us-west-2' 
                          FORMAT AS JSON {}
                          TIMEFORMAT as 'epochmillisecs';
""").format(config.get("S3","LOG_DATA"),
            config.get("IAM_ROLE","ARN"),
            config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""copy staging_songs
                         from {}
                         credentials 'aws_iam_role={}'
                         region 'us-west-2'
                         FORMAT AS JSON 'auto';
""").format(config.get("S3","SONG_DATA"),
            config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT e.ts, e.userid, e.level, s.song_id, s.artist_id, e.sessionid, s.artist_location, e.useragent
                            FROM staging_songs s JOIN staging_events e
                            ON s.title = e.song
                            AND s.artist_name = e.artist;
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT userid, firstname, lastname, gender, level
                        FROM staging_events
                        WHERE userid IS NOT NULL
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT ts, extract(hour from ts), extract(day from ts), extract(week from ts),
                        extract(month from ts), extract(year from ts), extract(weekday from ts)
                        FROM staging_events
                        WHERE ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
