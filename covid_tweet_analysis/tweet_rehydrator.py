import csv
import json
from pydoop import hdfs
from kafka import KafkaProducer
from pprint import pprint
from time import sleep
from random import randint

from covid_tweet_analysis.twitter import twitter_api_client


class GetTweetsProducer:
    
    def __init__(self, address: str, port: int, topic: str):
        self.producer = KafkaProducer(bootstrap_servers=f'{address}:{port}')
        self.topic = topic
        self.tweet_rules = {"ids": "1138505981460193280", "tweet.fields": "created_at"}
    
    
    def send_message(self, message):
        self.producer.send(self.topic, value=message)
    
    
    def run(self):
        client_hdfs = hdfs.hdfs(host='localhost', port=9000)
        dir = '/user/matt/input/tweets'
        files = client_hdfs.list_directory(dir)
        
        for file in files:
            with client_hdfs.open_file(file['path'], mode='rt') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter='\t')
                next(csv_reader)
                for row in csv_reader:
                    tweet_id = row[0]
                    tweet_rules = {"ids": f"{tweet_id}",
                                   "tweet.fields": "author_id,created_at,id,text"}
                    response = twitter_api_client.get_tweets(tweet_rules)
                    if 'data' in response:
                        self.send_message(json.dumps(response).encode('UTF-8'))
                        print('SENT MESSAGE:')
                        pprint(response)
                        print()
                    sleep(randint(1,2))


if __name__ == "__main__":
    
    print('init producer...')
    producer = GetTweetsProducer('localhost', 9092, 'getTweets')
    print('producer started.')
    producer.run()
