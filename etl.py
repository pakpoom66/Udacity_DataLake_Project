import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create SparkSession and Config to use in ETL as below:
    
    - Config spark.jars.packages to org.apache.hadoop:hadoop-aws:2.7.3
    - Config spark.hadoop.fs.s3a.impl to org.apache.hadoop.fs.s3a.S3AFileSystem
    - Config spark.hadoop.fs.s3a.awsAccessKeyId to os.environ['AWS_ACCESS_KEY_ID']
    - Config spark.hadoop.fs.s3a.awsSecretAccessKey to os.environ['AWS_SECRET_ACCESS_KEY']
    - Config spark.hadoop.fs.s3a.multiobjectdelete.enable to false
    
    """ 
        
    spark = SparkSession \
        .builder \
        .appName("Udacity_Project_DataLake_Phakphoom") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """  
    Read data from json file from S3, ETL the data, and write to parquet files in S3 (song, and artist table)
    
    - Song data (s3://udacity-dend/song_data) => song_data
    
    INPUT:
    - spark : pyspark.sql.SparkSession. That use Spark session
    - input_data : string. S3 Location of source data
    - output_data : string. S3 Location of target data
    
    REFERENCE:
    1. Idea and Sample for how to read configure file ... [Udacity: I cannot read the dl.cfg file](https://knowledge.udacity.com/questions/368715)
    2. Idea and Sample for how to write parquet files and partition ...
        - [Udacity: Can someone please tell me what is wrong with this code? I'm unable to execute this.](https://knowledge.udacity.com/questions/678149)
        - [Udacity: Error writing songs table to parquet files; folders get created in the bucket, but still get error](https://knowledge.udacity.com/questions/295862)
        - [Udacity: Data Lake Project 4: No data in songplays table](https://knowledge.udacity.com/questions/742420)
        - [Sparkbyexamples: PySpark Read and Write Parquet File](https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/)
        - [Udacity: How to write parquet Songplay table with year and month partition](https://knowledge.udacity.com/questions/60112)
    3. Idea and Sample for aggregate value in PySpark dataframe ... [Stackoverflow: Store aggregate value of a PySpark dataframe column into a variable](https://stackoverflow.com/questions/36987454/store-aggregate-value-of-a-pyspark-dataframe-column-into-a-variable)
    4. Idea and Sample for how to execute ETL of Data Lake project ... [Udacity: How to execute ETL of Data Lake project on Spark Cluster in AWS?](https://knowledge.udacity.com/questions/46619#552992)
    5. Idea and Sample for issues to read AWS S3 from PySpark ... 
        - [Udacity: S3 AWS Error Message: Forbidden](https://knowledge.udacity.com/questions/137494)
        - [Udacity: Error when trying to access udacity-dend s3a bucket](https://knowledge.udacity.com/questions/369097)
        - [Udacity: Unable to access S3 folders](https://knowledge.udacity.com/questions/374581)
        - [Stackoverflow: Spark 1.6.1 S3 MultiObjectDeleteException](https://stackoverflow.com/questions/38750638/spark-1-6-1-s3-multiobjectdeleteexception)
        - [Udacity: My etl.py cannot access to s3 for read data to testing in the project](https://knowledge.udacity.com/questions/759355)
    
    """ 
    
    # get filepath to song data file
    # Use this for provide manual path
    song_data = input_data + "song_data/*/*/*/*.json"
    #song_data = input_data + "song_data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    #df = spark.read.load(song_data)

    df.createOrReplaceTempView("song_data")

    # extract columns to create songs table 
    songs_table = spark.sql("""
        SELECT DISTINCT song_id, title, artist_id, year, duration FROM song_data
    """)
    
    # Count records of songs table for data quality checks
    print('\n***** Summary records each tables *****')
    count_songs_table = songs_table.count()
    print('The data in "{}" table have {} record(s)'.format('songs', count_songs_table))
    
    # write songs table to parquet files partitioned by year and artist
    songs_location= os.path.join(output_data, "songs")
    songs_table.write.partitionBy("year", "artist_id").parquet(songs_location, mode="overwrite")

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM song_data
    """)
    
    # Count records of artists table for data quality checks
    count_artists_table = artists_table.count()
    print('The data in "{}" table have {} record(s)'.format('artists', count_artists_table))
    
    # write artists table to parquet files
    artists_location= os.path.join(output_data, "artists")
    artists_table.write.parquet(artists_location, mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """   
    Read data from json file from S3, ETL the data, and Insert/Load to parquet files in S3 (time, user, and songplay table)
    
    - Log data (s3://udacity-dend/log_data) => log_data
    - Song data (s3://udacity-dend/song_data) => song_data
    
    INPUT:
    - spark : pyspark.sql.SparkSession. That use Spark session
    - input_data : string. S3 Location of source data
    - output_data : string. S3 Location of target data
    
    REFERENCE:
    1. Idea and Sample for how to read configure file ... [Udacity: I cannot read the dl.cfg file](https://knowledge.udacity.com/questions/368715)
    2. Idea and Sample for how to write parquet files and partition ...
        - [Udacity: Can someone please tell me what is wrong with this code? I'm unable to execute this.](https://knowledge.udacity.com/questions/678149)
        - [Udacity: Error writing songs table to parquet files; folders get created in the bucket, but still get error](https://knowledge.udacity.com/questions/295862)
        - [Udacity: Data Lake Project 4: No data in songplays table](https://knowledge.udacity.com/questions/742420)
        - [Sparkbyexamples: PySpark Read and Write Parquet File](https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/)
        - [Udacity: How to write parquet Songplay table with year and month partition](https://knowledge.udacity.com/questions/60112)
    3. Idea and Sample for filter by actions for song plays ... [Udacity: Project 4: Data Lake-# filter by actions for song plays](https://knowledge.udacity.com/questions/105430)
    4. Idea and Sample for how to convert and function to Datetime datatype ... 
        - [Udacity: Why I need convert timestamp column to timestamp? (step in project 4)](https://knowledge.udacity.com/questions/536246)
        - [Stackoverflow: How to register UDF with no argument in Pyspark](https://stackoverflow.com/questions/41328702/how-to-register-udf-with-no-argument-in-pyspark)
        - [Stackoverflow: PySpark add a column to a DataFrame from a TimeStampType column](https://stackoverflow.com/questions/30882268/pyspark-add-a-column-to-a-dataframe-from-a-timestamptype-column)
        - [GET HOURS, MINUTES, SECONDS AND MILLISECONDS FROM TIMESTAMP IN PYSPARK](https://www.datasciencemadesimple.com/get-hours-minutes-seconds-and-milliseconds-from-timestamp-in-pyspark/)
        - [pyspark.sql.functions.dayofweek](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.dayofweek.html)
    5. Idea and Sample for how to create column for Auto Increment ... 
        - [Stackoverflow: Auto - Incrementing pyspark dataframe column values](https://stackoverflow.com/questions/50174227/auto-incrementing-pyspark-dataframe-column-values)
        - [Towardsdatascience: Adding sequential IDs to a Spark Dataframe](https://towardsdatascience.com/adding-sequential-ids-to-a-spark-dataframe-fa0df5566ff6)
    6. Idea for PySpark Join Types ... [Sparkbyexamples: PySpark Join Types | Join Two DataFrames](https://sparkbyexamples.com/pyspark/pyspark-join-explained-with-examples/)
    7. Idea and Sample for aggregate value in PySpark dataframe ... [Stackoverflow: Store aggregate value of a PySpark dataframe column into a variable](https://stackoverflow.com/questions/36987454/store-aggregate-value-of-a-pyspark-dataframe-column-into-a-variable)
    8. Idea and Sample for how to execute ETL of Data Lake project ... [Udacity: How to execute ETL of Data Lake project on Spark Cluster in AWS?](https://knowledge.udacity.com/questions/46619#552992)
    9. Idea and Sample for issues to read AWS S3 from PySpark ... 
        - [Udacity: S3 AWS Error Message: Forbidden](https://knowledge.udacity.com/questions/137494)
        - [Udacity: Error when trying to access udacity-dend s3a bucket](https://knowledge.udacity.com/questions/369097)
        - [Udacity: Unable to access S3 folders](https://knowledge.udacity.com/questions/374581)
        - [Stackoverflow: Spark 1.6.1 S3 MultiObjectDeleteException](https://stackoverflow.com/questions/38750638/spark-1-6-1-s3-multiobjectdeleteexception)
        - [Udacity: My etl.py cannot access to s3 for read data to testing in the project](https://knowledge.udacity.com/questions/759355)
    
    """ 
    
    # get filepath to log data file
    # Use this for provide manual path
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    #df = spark.read.load(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("log_data")

    # extract columns for users table    
    users_table = spark.sql("""
        SELECT DISTINCT userId, firstName, lastName, gender, level FROM log_data WHERE userId IS NOT NULL
    """)
    
    # Count records of users table for data quality checks
    count_users_table = users_table.count()
    print('The data in "{}" table have {} record(s)'.format('users', count_users_table))
    
    # write users table to parquet files
    users_location= os.path.join(output_data, "users")
    users_table.write.parquet(users_location, mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    df.createOrReplaceTempView("log_data")
    time_table = spark.sql("""
        SELECT DISTINCT start_time FROM log_data
    """)
    
    # extract columns to create time table
    time_table = time_table.withColumn('hour',hour(time_table.start_time)) \
        .withColumn('day',dayofmonth(time_table.start_time)) \
        .withColumn('week',weekofyear(time_table.start_time)) \
        .withColumn('month',month(time_table.start_time)) \
        .withColumn('year',year(time_table.start_time)) \
        .withColumn('weekday',dayofweek(time_table.start_time)) 
    
    # Count records of time table for data quality checks
    count_time_table = time_table.count()
    print('The data in "{}" table have {} record(s)'.format('time', count_time_table))
    
    # write time table to parquet files partitioned by year and month
    time_location= os.path.join(output_data, "time")
    time_table.write.partitionBy("year", "month").parquet(time_location, mode="overwrite")

    # read in song data to use for songplays table
    # Use this for provide manual path
    #song_data = input_data + "song_data/*/*/*/*.json"

    song_output = output_data + "songs/"
    
    #dftemp = spark.read.json(song_data)
    dftemp = spark.read.parquet(song_output)
    
    dftemp.createOrReplaceTempView("songs")
    song_df = spark.sql("""
        SELECT DISTINCT song_id, title, artist_id, year, duration FROM songs
    """)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.sort("start_time").join(song_df, (df.song == song_df.title), 'inner') \
        .select(monotonically_increasing_id().alias("songplay_id"),
            col("start_time"),
            col("userId").alias("user_id"),
            col("level"),
            col("song_id"),
            col("artist_id"),
            col("sessionId").alias('session_id'),
            col("location"),
            col("userAgent").alias("user_agent")
        ) \
        .withColumn("month", month(col("start_time"))) \
        .withColumn("year", year(col("start_time")))
    
    # Count records of songplays table for data quality checks
    count_songplays_table = songplays_table.count()
    print('The data in "{}" table have {} record(s)'.format('songplays', count_songplays_table))

    # write songplays table to parquet files partitioned by year and month
    songplays_location= os.path.join(output_data, "songplays")
    songplays_table.write.partitionBy("year", "month").parquet(songplays_location, mode="overwrite")


def main():
    """
    Create Spark Session and provide input and output location in S3. Its Start point for this ETL process. 
    
    - Read the config AWS credential from `dl.cfg`
    - Call create_spark_session() to create Spark Session for use in other function
    - Prepare input_data and output_data location in S3
    - Call process_song_data() function for Load json data from S3, ETL and write to parquet files in S3 (song, and artist table)
    - Call process_log_data() function for Load json data from S3, ETL and write to parquet files in S3 (time, user, and songplay table)
    - Finally, stop the Spark session
    
    """
    
    print("== The ETL Process is Start ==")
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://cnp66-bucket/sparkify_datalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    print("== The ETL Process is End!! ==")
    spark.stop()


if __name__ == "__main__":
    main()
