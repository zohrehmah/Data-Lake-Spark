import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ 
    Creates SparkSession and returns it. If SparkSession is already created it returns
    the currently running SparkSession.
    
    :param: None
    :Returns: SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description:
        Process the songs data files and create extract songs table and artist table data from it.
    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """
        
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_columns =  ["song_id","title", "artist_id","year", "duration"]
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = df.select(songs_columns).dropDuplicates(subset=['song_id'])
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_columns = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]

    artists_table = df.selectExpr(artists_columns).dropDuplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
    Description:
            Process the event log file and extract data for table time, users and songplays from it.
    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """
        
    # get filepath to log data file
    log_data =log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select("userId","firstName","lastName","gender","level").drop_duplicates(subset=['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)), TimestampType())
    df = df.withColumn('start_date', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table =df.withColumn("hour",hour("start_date"))\
                  .withColumn("day",dayofmonth("start_date"))\
                  .withColumn("week",weekofyear("start_date"))\
                  .withColumn("month",month("start_date"))\
                  .withColumn("year",year("start_date"))\
                  .withColumn("weekday",dayofweek("start_date"))\
                  .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates(subset=['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time_table/"), mode='overwrite', partitionBy=["year","month"])

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs/*/*/')
    
    songplays = df.join(song_df, df.song == song_df.title)
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays = songplays.withColumn('songplay_id', monotonically_increasing_id()) 
    songplays = songplays.join(time_table, songplays.start_date == time_table.start_date)\
                                .select("songplay_id", songplays.start_date, "userid", "level", "song_id","artist_id", "sessionid", "location", "useragent", "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays.write.parquet(os.path.join(output_data + "songplays/"), mode='overwrite', partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkifyds/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
