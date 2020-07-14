from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer, KafkaClient

from pathlib import Path
import json
import os
import csv

ROOT_PATH = os.getcwd()

# File containing the authentication keys
# [0] Consumer Key (API Key)
# [1] Consumer Secret (API Secret)
# [2] Bearer Token
# [3] Access Token
# [4] Access Token Secret
AUTH_FILE = os.path.join(ROOT_PATH, 'auth.tsv')

SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
TOPIC = "covid-tweets"
SLEEP_TIMER = 1000

# Reads Twitter authentication factors from the first row of the file
with open(AUTH_FILE, "r", encoding="utf-8") as af:
    csv_reader_af = csv.reader(af, delimiter='\t')
    twttr_auth = next(csv_reader_af)
    CONSUMER_KEY = twttr_auth[0]
    CONSUMER_SECRET = twttr_auth[1]
    ACCESS_TOKEN = twttr_auth[3]
    TOKEN_SECRET = twttr_auth[4]

HASHTAG = "#covid"


class Listener(StreamListener):
    def on_data(self, data):
        producer.send(TOPIC, data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)


kafka = KafkaClient(
    bootstrap_servers = SERVERS
)
producer = KafkaProducer(
    bootstrap_servers = SERVERS,
    acks = 1
)
l = Listener()
auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, TOKEN_SECRET)
print (on_data)
stream = Stream(auth, l)
stream.filter(track="covid")
