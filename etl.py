"""ETL Pipeline"""
import os
import glob
import psycopg2
import pandas as pd

from functools import partial

from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process data from song json file and transfer it into the songs and artists table

    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[["song_id", "title", "artist_id", "year", "duration"]].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(df[["artist_id", "artist_name", "artist_location",
                           "artist_latitude", "artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)



