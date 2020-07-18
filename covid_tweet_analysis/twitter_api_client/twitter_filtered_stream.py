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

from .twitter_authenticator import TwitterAuthenticator
from covid_tweet_analysis.utils.tcp_connector import TCPConnector

twttrauth = TwitterAuthenticator()
    
consumer_key = twttrauth.consumer_key # Add your API key here
consumer_secret = twttrauth.consumer_secret  # Add your API secret key here

stream_url = "https://api.twitter.com/labs/1/tweets/stream/filter"
rules_url = "https://api.twitter.com/labs/1/tweets/stream/filter/rules"

sample_rules = [
    { 'value': '#covid19 angry lang:en', 'tag': 'covid' },
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
    print(response.status_code)
    return response

def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = str(full_tweet['text'].encode("utf-8"))
            print("Tweet Text: " + tweet_text)
            print("-------------------------------------------")
            tcp_connection.send(bytes(tweet_text + '\n', 'utf-8'))
        except:
            e = sys.exc_info()#[0]
            print("Error %s", e)

bearer_token = BearerTokenAuth(consumer_key, consumer_secret)

def setup_rules(auth):
    current_rules = get_all_rules(auth)
    delete_all_rules(current_rules, auth)
    set_rules(sample_rules, auth)


# Comment this line if you already setup rules and want to keep them
setup_rules(bearer_token)


# Connect to socket
resp = stream_connect(bearer_token)
tcp_conn = TCPConnector()
send_tweets_to_spark(resp, tcp_conn.connect_to_socket("localhost", 9009))


# Listen to the stream.
# This reconnection logic will attempt to reconnect when a disconnection is detected.
# To avoid rate limites, this logic implements exponential backoff, so the wait time
# will increase if the client cannot reconnect to the stream.
timeout = 0
while True:
    stream_connect(bearer_token)
    time.sleep(2 ** timeout)
    timeout += 1

