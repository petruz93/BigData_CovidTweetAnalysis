from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext, SparkSession
import sys
import requests
from pprint import pprint


class SparkStreamingConnector:

    def getDataStreamFromTCP(self, ssc:StreamingContext, tcpAddress:str="localhost", tcpPort:str=9009):
        dataStream = ssc.socketTextStream(tcpAddress, tcpPort)
        return dataStream


    # # Sets the session as a global variable
    # def getSparkSessionInstance(self, sparkConf):
    #     if ('sparkSessionSingletonInstance' not in globals()):
    #         globals()['sparkSessionSingletonInstance'] = SparkSession \
    #             .builder \
    #             .config(conf=sparkConf) \
    #             .getOrCreate()
    #     return globals()['sparkSessionSingletonInstance']

