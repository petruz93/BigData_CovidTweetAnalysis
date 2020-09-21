import sys
import requests
from pprint import pprint
from spark_streaming_connector import SparkStreamingConnector


class TwitterSparkStreamJob:

    def __init__(self, dataStream):
        self.dataStream = dataStream
        self.session = dataStream.context().getActive()

    def rddToHashtagCountsDF(self, time, rdd):
        print("----------- %s -----------" % str(time))
        try:
            # Get spark session singleton context from the current context
            # session = self.sparkStreamingConnector.getSparkSessionInstance(rdd.context.getConf())
            # session = rdd.context.getConf()
            # convert the RDD to Row RDD
            row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
            # create a DF from the Row RDD
            hashtags_df = self.session.createDataFrame(row_rdd)
            # Register the dataframe as table
            hashtags_df.createOrReplaceTempView("tempView")
            # get the top 10 hashtags from the table using SQL and print them
            hashtag_counts_df = self.session.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
            # if(store==True):
                # send df to database
            sendDFToDatabase(hashtag_counts_df, "hashtags", "covidstream")
            hashtag_counts_df.show()
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)


    def sendDFToDatabase(self, df, tableName:str, keyspaceName:str):
        if(df>0):
            df.write\
                .format("org.apache.spark.sql.cassandra")\
                .mode("append")\
                .options(tableName=tableName, keyspace=keyspaceName)\
                .save()

    def countHashtagWithinWindow(self):
        # split each tweet into words
        words = self.dataStream.flatMap(lambda line: line.split(" "))
        # filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
        hashtags = words.filter(lambda w: w and w[0]=='#').map(lambda x: (x, 1))
        # adding the count of each hashtag to its last count
        # tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
        tags_totals = hashtags.reduceByKeyAndWindow(func=(lambda a, b: a + b), invFunc=None, windowDuration=20)
        # do processing for each RDD generated in each interval
        tags_totals.foreachRDD(self.rddToHashtagCountsDF)
        #SparkStreamingConnector().start()