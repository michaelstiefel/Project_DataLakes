import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofweek, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    #song_data = input_data + 'song_data/*/*/*/*.json'
    song_data = input_data + 'song_data/*/*/*/*.json'
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist').parquet(os.path.join(output_data, 'songs_table'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'name', 'location', 'latitude', 'longitude').dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists_table'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where('page' == 'NextSong')

    # extract columns for users table
    users_table = df.select('user_id', 'first_name', 'last_name', 'gender', 'level').dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users_table"), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select("datetime", "timestamp") \
                  .withColumn("start_time", df.timestamp) \
                  .withColumn("hour", hour(df.datetime)) \
                  .withColumn("day", dayofmonth(df.datetime)) \
                  .withColumn("week", weekofyear(df.datetime)) \
                  .withColumn("month", month(df.datetime)) \
                  .withColumn("year", year(df.datetime)) \
                  .withColumn("weekday",  dayofweek(df.datetime))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time_table"), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = df.join(song_df, (df.artist == song_df.artist_name) & (df.song == song_df.title)) \
                    .withColumn("start_time", df.start_time) \
                    .withColumn("user_id", df.userId) \
                    .withColumn("level", df.level) \
                    .withColumn("song_id", song_df.song_id) \
                    .withColumn("artist_id", song_df.artist_id) \
                    .withColumn("session_id", df.sessionId) \
                    .withColumn("location", df.location) \
                    .withColumn("user_agent", df.userAgent)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songplays_table"), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://michael-datalakes/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
