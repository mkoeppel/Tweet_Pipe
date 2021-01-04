"""
allows authentification for the twitter API
and obtains tweets for a specific topic to be stored in MongoDB
"""

from datetime import datetime, timedelta

import json
import logging
import pymongo
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import credentials  # imports a python-file named 'credentials.py' from the SAME directory

## Data-Stream: Infinte flow of data as tweets continue to be generated all the time!

client = pymongo.MongoClient(host="mongodb", port=27017)

db_mongo = (
    client.twitter
)


def authenticate():
    """Function for handling Twitter Authentication. Please note
       that this script assumes you have a file called config.py
       which stores the 4 required authentication tokens:

       1. CONSUMER_API_KEY
       2. CONSUMER_API_SECRET
       3. ACCESS_TOKEN
       4. ACCESS_TOKEN_SECRET

    """
    auth = OAuthHandler(credentials.CONSUMER_API_KEY, credentials.CONSUMER_API_SECRET)
    auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)

    return auth


class TwitterListener(StreamListener):  # requiered for the tweepy-library to work
    def on_data(
        self, data
    ):  # this is called whenever new tweet arives/ data is the tweet itself, as json-file
        """Whatever we put in this method defines what is done with
        every single tweet as it is intercepted in real-time"""
        t = json.loads(
            data
        )
        text = t["text"]
        if "extended_tweet" in t:
            text = t["extended_tweet"]["full_text"]

        keyword = None
        for key in [
            "covid-19",
            "vaccine",
            "pandemic",
            "Pfizer",
            "Biontech",
            "AstraZeneca",
            "Moderna",
        ]:
            if (key in text) or (key in t["entities"]["hashtags"]):
                keyword = key
        tweet = {
            "text": text,
            "user_name": t["user"]["screen_name"],
            "followers_count": t["user"]["followers_count"],
            "location": t["user"]["location"],
            "reply_count": t["reply_count"],
            "retweet_count": t["retweet_count"],
            "keyword": keyword,
            "timestamp": datetime.strptime(
                t["created_at"], "%a %b %d %H:%M:%S +0000 %Y"
            ),
        }
        #
        # print(text + '\n\n') # instead of the logging.critical below
        db_mongo.twitter.insert(tweet)
        logging.critical("tweet added to mondoDB")
        logging.critical(f'\n\n\nTWEET INCOMING: {tweet["text"]}\n\n\n')

    def on_error(self, status):

        if status == 420:
            print(status)
            return False


if __name__ == "__main__":

    auth = authenticate()  # log in into twitter
    listener = TwitterListener()  # initiate a listener class
    stream = Stream(auth, listener)  # starts an inifnite loop listening to twitter
stream.filter(
    track=[
        "covid-19",
        "vaccine",
        "pandemic",
        "Pfizer",
        "Biontech",
        "AstraZeneca",
        "Moderna",
    ],
    languages=["en"],
)  # select those tweets your are interested in, by key-word and language
