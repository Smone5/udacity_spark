import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import to_timestamp, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date




config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['default']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['default']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*'
    
    # read song data file
    song_schema = R([Fld("artist", Str()),
                     Fld("artist_id", Str()),
                     Fld("artist_latitude", Dbl()),
                     Fld("artist_location", Str()),
                     Fld("artist_longitude", Dbl()),
                     Fld("artist_name", Str()),
                     Fld("duration", Dbl()),
                     Fld("num_songs", Int()),
                     Fld("song_id", Str()),
                     Fld("title", Str()),
                     Fld("year", Int()),  
                     ])
    print("Reading song data")
    df = spark.read.json(song_data, song_schema)

    print("Creating songs table")
    # extract columns to create songs table
    cols = ['song_id','title', 'artist_id', 'year','duration']
    songs_table = df.select(cols)
    songs_table = songs_table.drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.repartition('year','artist_id')
    
    songs_table_output = output_data+"songs_table/songs_table.parquet"
    songs_table.write.parquet(songs_table_output)

    print("Creating artist table")
    # extract columns to create artists table
    cols = ['artist_id','artist_name as name', 'artist_location as location', 'artist_latitude as latitude','artist_longitude as longitude']
    artist_table = df.selectExpr(cols)
    artist_table = artist_table.drop_duplicates()
    
            
    # write artists table to parquet files
    artist_table_output = output_data+"artist_table/artist_table.parquet"
    artist_table.write.parquet(artist_table_output)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    
    log_data = "s3a://udacity-dend/log_data/*"

    # read log data file
    log_schema = R([Fld("artist", Str()),
                     Fld("auth", Str()),
                     Fld("firstName", Str()),
                     Fld("gender", Str()),
                     Fld("itemInSession", Int()),
                     Fld("lastName", Str()),
                     Fld("length", Dbl()),
                     Fld("level", Str()),
                     Fld("location", Str()),
                     Fld("method", Str()),
                     Fld("page", Str()),
                     Fld("registration", Str()),
                     Fld("sessionId", Int()),
                     Fld("song", Str()),
                     Fld("status", Int()),
                     Fld("ts", Str()),
                     Fld("userAgent", Str()),
                     Fld("userId", Str(), False)    
                     ])
    
    print("Reading log data")
    df = spark.read.json(log_data, log_schema)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    print("Creating users table")
    # extract columns for users table 
    cols = ['userId as user_id','firstName as first_name', 'lastName as last_name', 'gender', 'level']
    users_table = df.selectExpr(cols)
    users_table = users_table.drop_duplicates()
    
    # write users table to parquet files
    users_table_output = output_data+"users_table/users_table.parquet"
    users_table.write.parquet(users_table_output)

    print("Creating time table")
    
    # create timestamp column from original timestamp column
    def parse_time(line : str) -> str:
        return(line[0:-3])
    
    parse_time_udf = udf(lambda epoch: parse_time(epoch), Str())
    df = df.withColumn("start_time", to_timestamp(from_unixtime(parse_time_udf(col("ts")))))
    
    # create datetime column from original timestamp column
    parse_time_udf = udf(lambda epoch: parse_time(epoch), Str())
    df = df.withColumn("date", to_date(from_unixtime(parse_time_udf(col("ts")))))
    
    # extract columns to create time table
    time_table = df.select('start_time')
    time_table = time_table.withColumn('hour', hour(time_table.start_time))
    time_table = time_table.withColumn('day', dayofmonth(time_table.start_time))
    time_table = time_table.withColumn('week', weekofyear(time_table.start_time))
    time_table = time_table.withColumn('month', month(time_table.start_time))
    time_table = time_table.withColumn('year', year(time_table.start_time))
    time_table = time_table.withColumn("weekday", date_format(df.start_time, "EEEE"))
    time_table = time_table.drop_duplicates()
    
    
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.repartition('year','month')
    
    time_table_output = output_data+"time_table/time_table.parquet"
    time_table.write.parquet(time_table_output)
    

    # read in song data to use for songplays table
    song_data = input_data+'song_data/*/*/*/*'
    
    song_schema = R([Fld("artist", Str()),
                     Fld("artist_id", Str()),
                     Fld("artist_latitude", Dbl()),
                     Fld("artist_location", Str()),
                     Fld("artist_longitude", Dbl()),
                     Fld("artist_name", Str()),
                     Fld("duration", Dbl()),
                     Fld("num_songs", Int()),
                     Fld("song_id", Str()),
                     Fld("title", Str()),
                     Fld("year", Int()),  
                     ])
    
    print("Creating songplays table")
    song_df = spark.read.json(song_data, song_schema)

    # extract columns from joined song and log datasets to create songplays table
    df.createOrReplaceTempView("e")
    song_df.createOrReplaceTempView("song_stage")
    songplays_table = spark.sql("""
    SELECT
        CAST(e.userId || e.sessionId || e.itemInSession as bigint) as songplay_id,
        e.ts start_time,
        CAST(e.userId as int) as user_id,
        e.level as level,
        st.song_id,
        st.artist_id,
        CAST(e.itemInSession as int) as session_id,
        e.location as location,
        e.userAgent as user_agent
    FROM e
    LEFT JOIN (select distinct title, artist_name, artist_id, duration, song_id from song_stage) as st
        ON ( e.song = st.title and e.artist = st.artist_name and e.length = st.duration)
    ORDER BY start_time ASC
    """)
    songplays_table = songplays_table.withColumn("start_time", to_timestamp(from_unixtime(parse_time_udf(col("start_time")))))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.repartition( year(songplays_table.start_time),month(songplays_table.start_time))
    
    songsplay_output = output_data+"time_table/time_table.parquet"
    songplays_table.write.parquet(songsplay_output)
    
    


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-datawharehouse23/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
