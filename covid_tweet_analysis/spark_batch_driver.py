from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import explode
import argparse

from covid_tweet_analysis.config.read_config import getConfigValue
from covid_tweet_analysis.utils.connectors import CassandraConnector as cassandra


parser = argparse.ArgumentParser()

ALL_OUTPUT_NAMES=["console", "cassandra"]
parser.add_argument('--output',
                    choices=ALL_OUTPUT_NAMES,
                    help="Possible outputs for data {}".format(ALL_OUTPUT_NAMES),
                    required=False
)
args = parser.parse_args()

# init spark job
sparkConf = SparkConf()\
        .setAppName("TwitterStreamAppCovid")\
        .setMaster("local[8]")\
        .setAll([("spark.cassandra.connection.host", "127.0.0.1"),\
            ("spark.sql.extentions", "com.datastax.spark.connector.CassandraSparkExtensions"),\
            ("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0"),\
            ("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog"),\
        ])

spark = SparkSession.builder\
    .config(conf=sparkConf)\
    .getOrCreate()


# create cassandra Keyspace and Column-families
spark.sql("CREATE DATABASE IF NOT EXISTS mycatalog.covid_tweets WITH DBPROPERTIES (class='SimpleStrategy', replication_factor='1')")
spark.sql("CREATE TABLE IF NOT EXISTS mycatalog.covid_tweets.batch_tweets (id STRING, author_id STRING, text STRING, created_at STRING) USING cassandra PARTITIONED BY (id)")

schemaUrl = getConfigValue("tweet", "getTweetsSchemaUrl")
tweetSchema = spark.read.json(schemaUrl).schema

# read tweets from kafka
lines = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "getTweets") \
    .load()
lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# perform query
query = lines.selectExpr( "CAST(value AS STRING) as jsonData") \
    .select(from_json("jsonData", tweetSchema).alias("data")) \
    .select("data.*")

tweet = query.selectExpr("data[0].id as id", "data[0].author_id as author_id", "data[0].text as text", "data[0].created_at as created_at") \
    .select("id", "author_id", "text", "created_at")

# write output
def write_output(output, data):
    if(output == "cassandra"):
        cassandra.write_df(data, "batch_tweets", "covid_tweets", True)
    else:
        data.write \
            .format("console") \
            .mode("append") \
            .save()

write_output(args.output, tweet)
