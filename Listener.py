from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer, KafkaClient

from pathlib import Path

root_path = Path('.')
auth_path = root_path / "auth.txt"

print (root_path)

access_token = ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""

class Listener(StreamListener):
    def on_data(self, data):
        producer.send("covid", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient(bootstrap_servers="localhost:9092")
producer = KafkaProducer(bootstrap_servers="localhost:9092")
l = Listener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track="covid")
