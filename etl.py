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


def process_data(cur, conn, filepath, func):
    """
    Read all the files from song ang log directory
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


