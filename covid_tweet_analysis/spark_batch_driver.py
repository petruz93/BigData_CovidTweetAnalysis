from pyspark import SparkConf
from pyspark.sql import SparkSession
import argparse

from covid_tweet_analysis.utils.connectors import CassandraConnector as cassandra


parser = argparse.ArgumentParser()

ALL_OUTPUT_NAMES=["console", "cassandra"]
parser.add_argument('--output',
                    choices=ALL_OUTPUT_NAMES,
                    help="Possible outputs for data {}".format(ALL_OUTPUT_NAMES),
                    required=False
)
parser.add_argument('--k',
                    type=int,
                    help="Number of elements in Top-K result",
                    required=False
)
args = parser.parse_args()


# init spark job
sparkConf = SparkConf()\
        .setAppName("TwitterStreamAppCovid")\
        .setMaster("local[8]")\
        .setAll([("spark.cassandra.connection.host", "127.0.0.1"),\
            ("spark.sql.extentions", "com.datastax.spark.connector.CassandraSparkExtensions"),\
            ("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0"),\
            ("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog"),\
        ])

spark = SparkSession.builder\
    .config(conf=sparkConf)\
    .getOrCreate()


# create cassandra Keyspace and Column-families
spark.sql("CREATE DATABASE IF NOT EXISTS mycatalog.covid_tweets WITH DBPROPERTIES (class='SimpleStrategy', replication_factor='1')")
spark.sql("CREATE TABLE IF NOT EXISTS mycatalog.covid_tweets.topK_authors (author_id STRING, count BIGINT) USING cassandra PARTITIONED BY (author_id)")

# read data from cassandra
stream_tweets_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="stream_blob_tweets", keyspace="covid_tweets") \
    .load()
    
batch_tweets_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="batch_tweets", keyspace="covid_tweets") \
    .load()

union_df = stream_tweets_df \
    .unionByName(batch_tweets_df) \
    .distinct()
query = union_df.groupBy('author_id').count().orderBy("count", ascending=False)

# write output
def write_output(output, data):
    if(output == "cassandra"):
        cassandra.write_df(data, "topK_authors", "covid_tweets", True)
    else:
        data.write \
            .format("console") \
            .mode("append") \
            .save()

write_output(args.output, query)
