from kafka import KafkaProducer
# from kafka.errors import KafkaError
import requests
import json
from pprint import pprint

from covid_tweet_analysis.twitter import twitter_api_client


# class Producer:

#     def __init__(self, address: str, port: int, topic: str):
#         self.producer = KafkaProducer(bootstrap_server=address+':'+port)
#         self.topic = topic

#     def pushMessage(self, message):
#         self.producer.send(self.topic, message)


class TweetProducer:

    def __init__(self, address: str, port: int, topic: str, rules=None):
        self.producer = KafkaProducer(bootstrap_servers=f'{address}:{port}')
        self.topic = topic
        self.tweet_rules = [
            { 'value': '(covid19 OR COVID19 OR COVID-19 OR covid-19 OR coronavirus OR CORONAVIRUS OR Covid-19) -is:retweet lang:en', 'tag': 'coronavirus' },
            { 'value': '(covid19 OR COVID19 OR COVID-19 OR covid-19 OR coronavirus OR CORONAVIRUS OR Covid-19) -is:retweet lang:en', 'tag': 'COVID-19' },
            { 'value': '(covid19 OR COVID19 OR COVID-19 OR covid-19 OR coronavirus OR CORONAVIRUS OR Covid-19) -is:retweet lang:en', 'tag': 'Covid-19' },
            { 'value': '(covid19 OR COVID19 OR COVID-19 OR covid-19 OR coronavirus OR CORONAVIRUS OR Covid-19) -is:retweet lang:en', 'tag': 'covid19' },
        ]
        self.bearer_token = twitter_api_client.bearer_token
        
        if rules is not None:
            twitter_api_client.setup_rules(rules, self.bearer_token)
        else:
            twitter_api_client.setup_rules(self.tweet_rules, self.bearer_token)


    def send_message(self, message):
        self.producer.send(self.topic, value=message)

    
    def start_publish(self):
        while True:
            print('start publish')
            response = twitter_api_client.stream_connect(self.bearer_token)
            print()
            for response_line in response.iter_lines():
                if response_line:
                    print(json.loads(response_line))
                    print()
                    self.send_message(response_line)


if __name__ == "__main__":
    print('init producer...')
    publisher = TweetProducer('localhost', 9092, 'covid19')
    print('producer started.')
    publisher.start_publish()
