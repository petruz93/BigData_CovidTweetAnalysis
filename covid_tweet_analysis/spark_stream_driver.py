
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext, SparkSession
import sys
import requests
from pprint import pprint
import argparse
from os import path

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import from_json
from pyspark.sql.types import json

from covid_tweet_analysis.utils.connectors import CassandraConnector as cassandra
from covid_tweet_analysis.utils.connectors import SparkStreamingConnector

parser = argparse.ArgumentParser()
# cassandra = CassandraConnector()

ALL_SOURCE_NAMES=["kafka", "socket"]
ALL_OUTPUT_NAMES=["kafka", "console", "cassandra"]

parser.add_argument('--source',
                    choices=ALL_SOURCE_NAMES,
                    help="Possible data sources {}".format(ALL_SOURCE_NAMES),
                    required=False
)

parser.add_argument('--output',
                    choices=ALL_OUTPUT_NAMES,
                    help="Possible outputs for data {}".format(ALL_OUTPUT_NAMES),
                    required=False
)

args = parser.parse_args()

#172.18.0.2
sparkConf = SparkConf()\
        .setAppName("TwitterStreamAppCovid")\
        .setMaster("local[8]")\
        .setAll([("spark.cassandra.connection.host", "127.0.0.1"),\
            ("spark.sql.extentions", "com.datastax.spark.connector.CassandraSparkExtensions"),\
            ("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0"),\
            ("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog"),\
        ])

            # ("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")        

spark = SparkSession.builder\
    .config(conf=sparkConf)\
    .getOrCreate()            

sc = spark.sparkContext

# create cassandra Keyspace and Column-families
spark.sql("CREATE DATABASE IF NOT EXISTS mycatalog.covidstream WITH DBPROPERTIES (class='SimpleStrategy', replication_factor='1')")
spark.sql("CREATE TABLE IF NOT EXISTS mycatalog.covidstream.hashtags (hashtag STRING, count BIGINT) USING cassandra PARTITIONED BY (hashtag)")

twitterData = spark.read.json("/user/rastark/input/twitter.json")
twitterDataSchema = twitterData.schema
# pprint(twitterData.printSchema())


def readStream(source:str):
    if(source == 'kafka'):
        lines = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "covid19") \
            .load()
        lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    else:
        lines = spark \
            .readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 9009) \
            .load()
    return lines


lines = readStream(args.source)

# Split the lines into words

query = lines.selectExpr( "CAST(value AS STRING) as jsonData") \
    .select(from_json("jsonData", twitterDataSchema).alias("data")) \
    .select("data.*")


query = query.select(
   explode(split("data.text", """[,.\[\]\{\}?!\s;\"'\\]""")
   ).alias("hashtag")).filter("hashtag LIKE '#%'")

# Generate running word count
output = query.groupBy("hashtag").count().repartition(7, "hashtag").orderBy("count", ascending=False) 


def write_stream(output, data):
    if(output == "cassandra"):
        cassandra.write_df_stream(data, "hashtags", "covidstream", True)
    else:
        data.writeStream \
            .format("console") \
            .option("truncate", "false") \
            .outputMode("complete") \
            .start() \
            .awaitTermination()


write_stream(args.output, output)

# SPARK STREAMING
# # create the Streaming Context from the above spark context with the specified interval size seconds
# ssc = StreamingContext(sc, 5)
# sc.setLogLevel("ERROR")

# session = ssc.getOrCreate(checkpointPath="checkpoint_TwitterApp", setupFunc=setup_conn.getDataStreamFromTPC(ssc))

# # setting a checkpoint to allow RDD recovery
# ssc.checkpoint("checkpoint_TwitterApp")    


# # inizialize connector
# spark_conn = SparkStreamingConnector()

# # dataStreamTCP = ssc.socketTextStream("localhost", 9009)
# dataStreamTCP = spark_conn.getDataStreamFromTCP(ssc)

# def rddToHashtagCountsDF(time, rdd):
#     print("----------- %s -----------" % str(time))
#     try:
#         # Get spark session singleton context from the current context
#         # session = self.sparkStreamingConnector.getSparkSessionInstance(rdd.context.getConf())
#         # session = rdd.context.getConf()
#         # convert the RDD to Row RDD
#         row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
#         # create a DF from the Row RDD
#         hashtags_df = session.createDataFrame(row_rdd)
#         # Register the dataframe as table
#         hashtags_df.createOrReplaceTempView("tempView")
#         # get the top 10 hashtags from the table using SQL and print them
#         hashtag_counts_df = session.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
#         # if(store==True):
#         # send df to database
#         sendDFToDatabase(hashtag_counts_df, "hashtags", "covidstream")
#         hashtag_counts_df.show()
#     except:
#         e = sys.exc_info()
#         print("Error: %s" % e)


# def sendDFToDatabase(self, df, tableName:str, keyspaceName:str):
#     if(df>0):
#         df.write\
#             .format("org.apache.spark.sql.cassandra")\
#             .mode("append")\
#             .options(tableName=tableName, keyspace=keyspaceName)\
#             .save()

# # split each tweet into words
# words = dataStreamTCP.flatMap(lambda line: line.split(" "))
# # filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
# hashtags = words.filter(lambda w: w and w[0]=='#').map(lambda x: (x, 1))
# # adding the count of each hashtag to its last count
# # tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
# tags_totals = hashtags.reduceByKeyAndWindow(func=(lambda a, b: a + b), invFunc=None, windowDuration=20)
# # do processing for each RDD generated in each interval
# tags_totals.foreachRDD(rddToHashtagCountsDF)
# #SparkStreamingConnector().start()

# # # initialize job support class
# # # 
# # spark_job = TwitterSparkStreamJob(dataStreamTCP)

# # # execute hashtag job on TCP client
# # spark_job.countHashtagWithinWindow()

# # start the streaming computation
# ssc.start()
# # wait for the streaming to finish
# ssc.awaitTermination()