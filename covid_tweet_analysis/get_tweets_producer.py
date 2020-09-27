from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

import argparse

parser = argparse.ArgumentParser()
ALL_OUTPUT_NAMES=["kafka", "console"]
parser.add_argument('--output',
                    choices=ALL_OUTPUT_NAMES,
                    help="Possible outputs for data {}".format(ALL_OUTPUT_NAMES),
                    required=False
)
args = parser.parse_args()


# Spark application
sparkConf = SparkConf()\
        .setAppName("CovidTweetIdStreamApp")\
        .setMaster("local[8]")\
        .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")

spark = SparkSession.builder\
    .config(conf=sparkConf)\
    .getOrCreate()

fileSchema = StructType().add("tweet_id", "string").add("user_id", "string")
tsvDF = spark \
    .readStream \
    .option("sep", "\t") \
    .option("header", True) \
    .schema(fileSchema) \
    .csv("hdfs://localhost:9000/user/matt/input/tweets/")
    # .csv("/home/matt/Documents/bigdata/BigData_CovidTweetAnalysis/data")

df = tsvDF.select('tweet_id')


def write_output(output, data):
    if output == 'kafka':
        return data.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "tweetId")
    else:
        return data.writeStream \
            .format("console") \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/spark_streaming/get_tweets_producer")


write_output(args.output, df) \
    .start() \
    .awaitTermination()
