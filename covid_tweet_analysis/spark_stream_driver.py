
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

from covid_tweet_analysis.config.read_config import getConfigValue
from covid_tweet_analysis.utils.connectors import CassandraConnector as cassandra
from covid_tweet_analysis.utils.connectors import SparkStreamingConnector

parser = argparse.ArgumentParser()
# cassandra = CassandraConnector()

ALL_SOURCE_NAMES=["kafka", "socket"]
ALL_OUTPUT_NAMES=["kafka", "console", "cassandra"]

parser.add_argument('--source',
                    choices=ALL_SOURCE_NAMES,
                    help="Possible data sources {}".format(ALL_SOURCE_NAMES),
                    required=False,
                    default="socket"
)

parser.add_argument('--output',
                    choices=ALL_OUTPUT_NAMES,
                    help="Possible outputs for data {}".format(ALL_OUTPUT_NAMES),
                    required=False,
                    default="console"
)

# Da implementare correttamente
parser.add_argument('--del_checkpoint',
                    choices=["true", "false"],
                    help="If set to true, deletes spark checkpoint folder (DEFAULT false)",
                    required=False,
                    default="false"
)

args = parser.parse_args()

#172.18.0.2
sparkConf = SparkConf()\
        .setAppName("TwitterStreamAppCovid")\
        .setMaster("local[*]")\
        .setAll([("spark.cassandra.connection.host", "127.0.0.1"),\
            ("spark.executors.instances", "4"),\
            ("spark.sql.extentions", "com.datastax.spark.connector.CassandraSparkExtensions"),\
            ("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0"),\
            ("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog"),\
        ])      

spark = SparkSession.builder\
    .config(conf=sparkConf)\
    .getOrCreate()            

sc = spark.sparkContext

# create cassandra Keyspace and Column-families
spark.sql("CREATE DATABASE IF NOT EXISTS mycatalog.covid_tweets WITH DBPROPERTIES (class='SimpleStrategy', replication_factor='1')")
spark.sql("CREATE TABLE IF NOT EXISTS mycatalog.covid_tweets.stream_hashtags (hashtag STRING, count BIGINT) USING cassandra PARTITIONED BY (hashtag)")
spark.sql("CREATE TABLE IF NOT EXISTS mycatalog.covid_tweets.stream_blob_tweets (id STRING, author_id STRING, created_at STRING, text STRING) USING cassandra PARTITIONED BY (id)")

schemaUrl = getConfigValue("tweet", "streamTweetsSchemaUrl")
tweetSchema = spark.read.json(schemaUrl).schema


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


lines_1 = readStream(args.source)
lines_2 = readStream(args.source)

# Access the tweet data
# Query to get useful tweet fields
query_1 = lines_1.selectExpr( "CAST(value AS STRING) as jsonData") \
    .select(from_json("jsonData", tweetSchema).alias("data")) \
    .select("data.*")
tweet = query_1.selectExpr("data.id as id", "data.author_id as author_id", "data.created_at as created_at", "data.text as text")

# Query to count hashtags
query_2 = lines_2.selectExpr( "CAST(value AS STRING) as jsonData") \
    .select(from_json("jsonData", tweetSchema).alias("data")) \
    .select("data.*")

# Hashtag_count job
hashtag_count = query_2.select( \
    explode(split("data.text", """[,.\[\]\{\}\\?!\s;\"']""") \
    ).alias("hashtag")).filter("hashtag LIKE '#%'")
hashtag_count_output = hashtag_count.groupBy("hashtag").count().orderBy("count", ascending=False).repartition(4, "hashtag")


def write_stream(output_device, data, table=None, database=None, output_mode:str="complete", del_checkpoint=False):
    if(output_device == "cassandra"):
        cassandra.write_df_stream(data, table, database, output_mode, True)
    else:
        data.writeStream \
            .format("console") \
            .option("truncate", "true") \
            .outputMode(output_mode) \
            .start()
            # .awaitTermination()

write_stream(args.output, tweet, table="stream_blob_tweets", database="covid_tweets", output_mode="append"),
write_stream(args.output, hashtag_count_output, table="stream_hashtags", database="covid_tweets", output_mode="complete")
spark.streams.awaitAnyTermination()