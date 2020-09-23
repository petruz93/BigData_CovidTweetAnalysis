from kafka import KafkaConsumer
from pprint import pprint
import json


class TweetStreamConsumer:
    
    def __init__(self, address: str, port: int, topics):
        self.consumer = KafkaConsumer(topics, bootstrap_servers=f'{address}:{port}')
                                    #   , auto_offset_reset='earliest')
    
    def subscribe(self):
        for message in self.consumer:
            # pprint(json.loads(message.value))
            print(message.value.decode('utf-8'))


if __name__ == "__main__":
    print('init consumer...')
    subscriber = TweetStreamConsumer('localhost', 9092, 'covid19')
    print('consumer started.')
    subscriber.subscribe()
    