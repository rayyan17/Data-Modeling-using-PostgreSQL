"""Script for All Data Manipulation Queries"""


# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


drop_table_queries = [song_table_drop, artist_table_drop, time_table_drop, user_table_drop, songplay_table_drop]
