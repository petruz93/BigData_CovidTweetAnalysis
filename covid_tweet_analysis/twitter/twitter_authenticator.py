import os
import csv

class TwitterAuthenticator:

    def __init__(self):
        ROOT_PATH = os.getcwd()
        # File containing the authentication keys
        # [0] Consumer Key (API Key)
        # [1] Consumer Secret (API Secret)
        # [2] Bearer Token
        # [3] Access Token
        # [4] Access Token Secret
        AUTH_FILE = os.path.join(ROOT_PATH, 'covid_tweet_analysis/twitter/auth.tsv')

        # Reads Twitter authentication factors from the first row of the file
        with open(AUTH_FILE, "r", encoding="utf-8") as af:
            csv_reader_af = csv.reader(af, delimiter='\t')
            twttr_auth = next(csv_reader_af)
            CONSUMER_KEY = twttr_auth[0]
            CONSUMER_SECRET = twttr_auth[1]
            ACCESS_TOKEN = twttr_auth[3]
            TOKEN_SECRET = twttr_auth[4]
        self.consumer_key = CONSUMER_KEY
        self.consumer_secret = CONSUMER_SECRET
        self.access_token = ACCESS_TOKEN
        self.token_secret = TOKEN_SECRET