# Sparkify DataBase
Sparkify DataBase is designed to effeciently retrieve information about artist, there songs, and details related to it. All the sparkify database was available in a non-effecient file based system. For this purpose we built a pipeline to transfer all the data in SparkifyDB.


## Schema Design
Star Schema is used to build this database.
Songplays table is used as a Fact table and other tables users, songs, artists and time were acting as Dimension Tables expalining detailed information about each fact.


## ETL Pipeline
### Extraction:
We extract data for songs and related logs from the following directories:
```
song_data/
log_data/
```


### Transformation
From songs data we have transformed our data to fit in songs and artists table
From log_data we have transformed our data to fit in time and users data
Information from log_data and other tables were used to build the songsplays table

### Load
All the data from directories is transferred to PostgreSQL database


## Running the project
In order to run the project from the scratch run the following commands from your terminal:
```
python3.6 create_tables.py
```

then
```
python3.6 etl.py
```

In case, you have already created your tables and just want to add new data use only the second command.
