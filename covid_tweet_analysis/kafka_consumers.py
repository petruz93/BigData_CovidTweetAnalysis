from kafka import KafkaConsumer
from pprint import pprint
import json


class TweetStreamConsumer:
    
    def __init__(self, address: str, port: int, topics):
        self.consumer = KafkaConsumer(topics, bootstrap_servers=f'{address}:{port}'
                                      , auto_offset_reset='earliest'
                                      , value_deserializer=lambda m: json.loads(m)
                                    )
    
    
    def read_stream(self):
        for message in self.consumer:
            print(message.value)
            print()


if __name__ == "__main__":
    print('init consumer...')
    subscriber = TweetStreamConsumer('localhost', 9092, 'covid19')
    print('consumer started.')
    subscriber.read_stream()
