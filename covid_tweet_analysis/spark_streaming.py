from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext, SparkSession, DataFrame
import sys
import requests
from pprint import pprint

from .utils.connectors import CassandraConnector as cassandra


# CONTEXT DEFINITION
# create spark configuration
spark = SparkSession.builder\
    .appName("TwitterStreamAppCovid")\
    .master("local[2]")\
    .config("spark.cassandra.connection.host", "172.18.0.2")\
    .config("spark.sql.extentions", "com.datastax.spark.connector.CassandraSparkExtensions")\
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0-beta")\
    .config("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")\
    .getOrCreate() 
    
sc = spark.sparkContext


# create cassandra Keyspace and Column-families
spark.sql("CREATE DATABASE IF NOT EXISTS mycatalog.covidstream WITH DBPROPERTIES (class='SimpleStrategy', replication_factor='1')")
spark.sql("CREATE TABLE IF NOT EXISTS mycatalog.covidstream.hashtags (hashtag STRING, hashtag_count Int) USING cassandra PARTITIONED BY (hashtag)")

# create the Streaming Context from the above spark context with the specified interval size seconds
ssc = StreamingContext(sc, 10)
sc.setLogLevel("ERROR")

# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)

# initialize Cassandra "hashtag" table


# Sets the session as a global variable
def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


# TRANSFORMATION LOGIC
# Sums all the new_values for each hashtag and add them to the total_sum across 
# all the batches then saves the data into a tag_totals RDD
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


#SparkStreaming
def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark session singleton context from the current context
        session = getSparkSessionInstance(rdd.context.getConf())
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        # create a DF from the Row RDD
        hashtags_df = session.createDataFrame(row_rdd)
        # Register the dataframe as table
        hashtags_df.createOrReplaceTempView("hashtags")
        # get the top 10 hashtags from the table using SQL and print them
        hashtag_counts_df = session.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
        if(hashtag_counts_df.count()>0):
            cassandra.write(hashtag_counts_df,"hashtags", "covidstream", True)
        # send_df_to_database(hashtag_counts_df, session)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


# def send_df_to_database(df, session):
    # session.write\
    #     .format("org.apache.spark.sql.cassandra")\
    #     .mode('append')\
    #     .options(table="hashtags", keyspace="covidstream")\
    #     .save().show()  


# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))

# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))

# adding the count of each hashtag to its last count
# tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
tags_totals = hashtags.reduceByKeyAndWindow(func=(lambda a, b: a + b), invFunc=None, windowDuration=20)

# do processing for each RDD generated in each interval
tags_totals.foreachRDD(process_rdd)

# start the streaming computation
ssc.start()

# wait for the streaming to finish
ssc.awaitTermination()
