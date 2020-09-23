from kafka import KafkaProducer
# from kafka.errors import KafkaError
import requests

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


    def pushMessage(self, message):
        self.producer.send(self.topic, value=message)

    
    def startPublish(self):
        while True:
            print('start publish')
            messages = twitter_api_client.stream_connect(self.bearer_token)
            print('dopo connect')
            for m in messages:
                self.pushMessage(m)


if __name__ == "__main__":
    print('init producer...')
    publisher = TweetProducer('localhost', 9092, 'covid19')
    print('producer started.')
    publisher.startPublish()
