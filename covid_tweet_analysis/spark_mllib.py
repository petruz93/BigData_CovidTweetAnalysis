from pyspark.mllib import absolute_import
from .spark_streaming_connector import SparkStreamingConnector
import sys

#SparkStreaming methods


# TRANSFORMATION LOGIC
# Sums all the new_values for each hashtag and add them to the total_sum across 
# all the batches then saves the data into a tag_totals RDD
def aggregate_tags_count(self, new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

