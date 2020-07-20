import pyspark
import pyspark.sql
from pyspark.sql import SparkSession, Row
from pprint import pprint
import json
from operator import add

from covid_tweet_analysis.twitter_api_client.twitter_getTweets import get_tweets


spark = SparkSession.builder.appName('topK Hashtag').master('local').getOrCreate()

lines = spark.read.csv('data/en_ids_2020-05-01.tsv', sep='\t', header=True)
tweetIds = lines.select('tweet_id').head(100)

ids = []
for t in tweetIds:
    ids.append(t['tweet_id'])
ids = ','.join(ids)

# get tweets
params = {'ids': ids} #, "tweet.fields": "entities"}
tweets = get_tweets(params)
# pprint(tweets)
# for i,t in enumerate(tweets):
#     if t:
#         print('%d.\t%s' % (i, t['text']))

print('-----------------------------------')

# create data frame
df = spark.createDataFrame([Row(**t) for t in tweets])
# df.show()
onlyHashtagTweets = df.filter(df.text.contains('#')).select('text')
# onlyHashtagTweets.show(n=100)

hashtagCounts = onlyHashtagTweets.rdd \
    .flatMap(lambda row: row['text'].split(' ')) \
    .filter(lambda word: word and word[0] == '#') \
    .map(lambda word: (word, 1)) \
    .reduceByKey(add)

topK = hashtagCounts.sortBy(lambda x: x[1], False).take(10)#.collect()

pprint(topK)