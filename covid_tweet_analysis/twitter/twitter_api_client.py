import requests
from requests.auth import AuthBase
import json
from pprint import pprint

from covid_tweet_analysis.config.read_config import getConfigValue


# Bearer Token generator class
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


# get all configuration params
__consumer_key = getConfigValue('twitter', 'consumerKey')
__consumer_secret = getConfigValue('twitter', 'consumerSecret')
bearer_token = BearerTokenAuth(__consumer_key, __consumer_secret)


## Twitter connection functions

def get_all_rules(rules_url, auth):
    response = requests.get(rules_url, auth=auth)
    if response.status_code is not 200:
        raise Exception(f"Cannot get rules (HTTP %d): %s" % (response.status_code, response.text))
    return response.json()


def delete_all_rules(rules_url, rules, auth):
    if rules is None or 'data' not in rules:
        return None
    ids = list(map(lambda rule: rule['id'], rules['data']))
    payload = {
        'delete': { 'ids': ids }
    }
    response = requests.post(rules_url, auth=auth, json=payload)
    if response.status_code is not 200:
        raise Exception(f"Cannot delete rules (HTTP %d): %s" % (response.status_code, response.text))


def set_rules(rules_url, rules, auth):
    if rules is None:
        return
    payload = {
        'add': rules
    }
    response = requests.post(rules_url, auth=auth, json=payload)
    if response.status_code is not 201:
        raise Exception(f"Cannot create rules (HTTP %d): %s" % (response.status_code, response.text))


def setup_rules(rules, auth):
    rules_url = "https://api.twitter.com/labs/1/tweets/stream/filter/rules"
    current_rules = get_all_rules(rules_url, auth)
    delete_all_rules(rules_url, current_rules, auth)
    set_rules(rules_url, rules, auth)


def stream_connect(auth=bearer_token):
    stream_url = "https://api.twitter.com/labs/1/tweets/stream/filter"
    response = requests.get(stream_url, auth=auth, stream=True)
    print('response status:', response.status_code)
    # for response_line in response.iter_lines():
    #     if response_line:
    #         print(json.loads(response_line))
    return response


def get_tweets(params, auth=bearer_token):
    """
        Send a request to Twitter Api and returns a json object
        that contains all tweet details specified in params.
    """
    request_url = "https://api.twitter.com/labs/2/tweets"
    response = requests.get(request_url, params, auth=auth)
    print('response status:', response.status_code)
    return response.json()


def get_tweet(tweet_id, auth=bearer_token):
    """
        Send a request to Twitter Api and returns a json object
        that contains tweet details of that tweet_id.
    """
    request_url = "https://api.twitter.com/labs/2/tweets"
    response = requests.get(f'{request_url}/{tweet_id}', auth=auth)
    print('response status:', response.status_code)
    return response.json()
