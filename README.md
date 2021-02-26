# Project Overview
Sparkify is a music streaming startup with a growing user base and song database.

Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
An ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.

# Project Repository files
1. song_date file contains metadata about a song and the artist of that song. 
2. log_data file contains activity logs from a music streaming app based on specified configurations.

# Database Design
1. Song Dataset:
1.1 songs - song_id, title, artist_id, year, duration
1.2 artists - artist_id, name, location, latitude, longitude

2. Log dataset:
2.1 songplays - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
2.2 users - user_id, first_name, last_name, gender, level
2.3 Time -  start_time, hour, day, week, month, year, weekday

The songplays is a fact table and other tables are dimension.

# ETL
1. Read data from S3
Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
The script reads song_data and load_data from S3.

2.Process data using spark
Transforms them to create five different tables.
The source files reside in s3a://udacity-dend

3.Writes them to partitioned parquet files in table directories on S3 (s3a://sparkifyds/).

# How To Run the Project
python etl.py
