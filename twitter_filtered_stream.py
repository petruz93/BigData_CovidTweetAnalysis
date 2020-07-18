import json
import os
import time
from pprint import pprint
import socket
import requests
import requests_oauthlib
import sys

import requests
from requests.auth import AuthBase, HTTPBasicAuth

from twitter_authenticator import TwitterAuthenticator

twttrauth = TwitterAuthenticator()

consumer_key = twttrauth.consumer_key # Add your API key here
consumer_secret = twttrauth.consumer_secret  # Add your API secret key here

stream_url = "https://api.twitter.com/labs/1/tweets/stream/filter"
rules_url = "https://api.twitter.com/labs/1/tweets/stream/filter/rules"

sample_rules = [
    { 'value': '#covid19 happy lang:en', 'tag': 'covid pictures' },
]

# Gets a bearer token
class BearerTokenAuth(AuthBase):
    
    def __init__(self, consumer_key, consumer_secret):
        self.bearer_token_url = "https://api.twitter.com/oauth2/token"
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.bearer_token = self.get_bearer_token()

    def get_bearer_token(self):
        response = requests.post(
        self.bearer_token_url, 
        auth=(self.consumer_key, self.consumer_secret),
        data={'grant_type': 'client_credentials'},
        headers={'User-Agent': 'TwitterDevFilteredStreamQuickStartPython'})

        if response.status_code is not 200:
            raise Exception(f"Cannot get a Bearer token (HTTP %d): %s" % (response.status_code, response.text))

        body = response.json()
        return body['access_token']

    def __call__(self, r):
        r.headers['Authorization'] = f"Bearer %s" % self.bearer_token
        r.headers['User-Agent'] = 'TwitterDevFilteredStreamQuickStartPython'
        return r


def get_all_rules(auth):
    response = requests.get(rules_url, auth=auth)

    if response.status_code is not 200:
        raise Exception(f"Cannot get rules (HTTP %d): %s" % (response.status_code, response.text))

    return response.json()


def delete_all_rules(rules, auth):
    if rules is None or 'data' not in rules:
        return None

    ids = list(map(lambda rule: rule['id'], rules['data']))

    payload = {
        'delete': {
        'ids': ids
    }
}

    response = requests.post(rules_url, auth=auth, json=payload)

    if response.status_code is not 200:
        raise Exception(f"Cannot delete rules (HTTP %d): %s" % (response.status_code, response.text))

def set_rules(rules, auth):
    if rules is None:
        return

    payload = {
        'add': rules
    }

    response = requests.post(rules_url, auth=auth, json=payload)

    if response.status_code is not 201:
        raise Exception(f"Cannot create rules (HTTP %d): %s" % (response.status_code, response.text))

def stream_connect(auth):
    response = requests.get(stream_url, auth=auth, stream=True)
    # for response_line in response.iter_lines():
    #     if response_line:
    #         streamed_data = json.loads(response_line)
    #         pprint(streamed_data)
    return response

def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print("-------------------------------------------")
            tcp.connection.send(tweet_text + '\n')
        except:
            e = sys.exc_info()[0]
            print("Error %s", e)

bearer_token = BearerTokenAuth(consumer_key, consumer_secret)

def setup_rules(auth):
    current_rules = get_all_rules(auth)
    delete_all_rules(current_rules, auth)
    set_rules(sample_rules, auth)


# Comment this line if you already setup rules and want to keep them
setup_rules(bearer_token)

# Listen to the stream.
# This reconnection logic will attempt to reconnect when a disconnection is detected.
# To avoid rate limites, this logic implements exponential backoff, so the wait time
# will increase if the client cannot reconnect to the stream.
timeout = 0
while True:
    stream_connect(bearer_token)
    time.sleep(2 ** timeout)
    timeout += 1

# TCP connection to SparkStreaming
TCP_IP = "localhost"
TPC_PORT = 9009
conn = None 
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TPC_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = stream_connect(bearer_token)
send_tweets_to_spark(resp, conn)
