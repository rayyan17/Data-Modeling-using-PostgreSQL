"""Script for All Data Manipulation Queries"""


# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays \
                            (songplay_id serial PRIMARY KEY, start_time timestamp NOT NULL, user_id int NOT NULL, \
                            level varchar, song_id varchar, artist_id varchar, session_id int, \
                            location varchar, user_agent text);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users \
                        (user_id int PRIMARY KEY, first_name varchar, \
                        last_name varchar, gender varchar, level varchar);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs \
                        (song_id varchar PRIMARY KEY, title varchar, \
                        artist_id varchar, year int, duration numeric);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists \
                          (artist_id varchar PRIMARY KEY, name varchar, \
                          location varchar, latitude numeric, longitude numeric);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time \
                        (start_time timestamp PRIMARY KEY, hour int NOT NULL, \
                        day int NOT NULL, week int NOT NULL, month int NOT NULL, \
                        year int NOT NULL, weekda int NOT NULL);""")


drop_table_queries = [song_table_drop, artist_table_drop, time_table_drop, user_table_drop,
                      songplay_table_drop]

create_table_queries = [song_table_create, artist_table_create, time_table_create, user_table_create,
                        songplay_table_create]
