import os
import json
import requests
from pprint import pprint
from requests.auth import AuthBase

from covid_tweet_analysis.config.read_config import getConfigValue


# twttrAuth = TwitterAuthenticator()
# consumer_key = twttrAuth.consumer_key  # Add your API key here
# consumer_secret = twttrAuth.consumer_secret  # Add your API secret key here
consumer_key = getConfigValue('twitter', 'consumerKey')
consumer_secret = getConfigValue('twitter', 'consumerSecret')

request_url = "https://api.twitter.com/labs/2/tweets"
# params = {"ids": "1138505981460193280"} #, "tweet.fields": "created_at"}

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
            data={'grant_type': 'client_credentials'})

        if response.status_code is not 200:
            raise Exception(f"Cannot get a Bearer token (HTTP %d): %s" % (
                response.status_code, response.text))

        body = response.json()
        return body['access_token']

    def __call__(self, r):
        r.headers['Authorization'] = f"Bearer %s" % self.bearer_token
        return r

def bearer_token():
    return BearerTokenAuth(consumer_key, consumer_secret)


def get_tweets(params, auth=None):
    """
        Send a request to Twitter Api and returns a json object
        that contains all tweet details specified in params.
    """
    if auth == None:
        auth = bearer_token()
    response = requests.get(request_url, params, auth=auth)
    return response.json()['data']
    # result = []
    # for response_line in response.iter_lines():
    #     if response_line:
    #         result.append(json.loads(response_line))
    #         pprint(json.loads(response_line))
    # return result


def stream_connect(stream_url, auth=bearer_token()):
    print('dentro connect')
    response = requests.get(stream_url, auth=auth, stream=True)
    print(response.status_code)
    print('dopo la richiesta')
    return response#.json()
