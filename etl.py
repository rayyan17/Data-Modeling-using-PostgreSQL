"""ETL Pipeline"""
import configparser
import glob
import os
from functools import partial

import pandas as pd
import psycopg2

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


def process_log_file(cur, filepath):
    """
    Process data from log files and transfer data into time, user and songplays table
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]

    # convert timestamp column to datetime
    t = df["ts"].apply(partial(pd.Timestamp, unit="ms"))

    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekda")
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.Timestamp(row["ts"], unit="ms"), row["userId"],
                         row["level"], songid, artistid, row["sessionId"],
                         row["location"], row["userAgent"])

        cur.execute(songplay_table_insert, songplay_data)


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


def main():
    """
    - Connect to the Sparkify Database
    - Process all the song files and insert it into corresponding tables
    - Process all the log files and insert data into the corresponding tabless
    """
    config = configparser.ConfigParser()
    config.read('psql.cfg')
    db_config = config["DATABASE"]

    conn = psycopg2.connect("host={} dbname={} user={} password={}".format(db_config["HOST"],
                                                                           db_config["OUTPUT_DB_NAME"],
                                                                           db_config["DB_USER"],
                                                                           db_config["DB_PASSWORD"]))
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
