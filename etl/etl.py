'''
1. Extract data from MongoDB
- Connect to the database
- Query the data

2. Transform the data
- Sentiment Analysis (will see this in the afternoon)
- Possibly we could transform datatypes

3. Load it into Postgres
- Connect to postgres
- Insert Into postgres
'''

import time
import logging

from os import environ
from dotenv import load_dotenv

from pymongo import MongoClient
from sqlalchemy import create_engine
import psycopg2
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

### Create connection to mongodb
client = MongoClient(host='mongodb', port=27017)
# connect to the twitter database
db_mongo = client.twitter
# connect to the tweets collection
tweets = db_mongo.tweets


### Create connection to postgresdb
conns = f"postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB_NAME}"
db_pg = create_engine(conns, echo=True)
#                   DB type    user     psw  host       port dbname


create_table = """
                CREATE TABLE IF NOT EXISTS tweets (
                user_name TEXT,
                followers_count NUMERIC,
                text TEXT,
                sentiment NUMERIC
                );
               """

db_pg.execute(create_table)

analyzer = SentimentIntensityAnalyzer()

def extract():
    '''Extracts tweets from the MongoDB database
    and mark them as extracted
    '''
    extracted_tweets = list(tweets.find({"extracted":"no"}))
    db_mongo.tweets.update_many({"extracted":"no"}, {"$set": {"extracted":"yes"}})
    return extracted_tweets


def transform(extracted_tweets):
    '''Transforms the data'''
    transformed_tweets = []
    for tweet in extracted_tweets:
        sentiment = analyzer.polarity_scores(tweet['text'])
        tweet['sentiment'] = sentiment['compound']
        transformed_tweets.append(tweet)
    return transformed_tweets


def load(transformed_tweets):
    '''Load transformed data into the postgres database'''
    for tweet in transformed_tweets:
        insert_query = "INSERT INTO tweets VALUES (%s, %s, %s, %s, %s, %s, %s)"
        db_pg.execute(insert_query, (tweet['user_name'], tweet['text'], tweet['followers_count'], tweet['place'], tweet['reply_count'], tweet['retweet_count'], tweet['sentiment']))
        logging.critical('---Inserted a new tweet into postgres---')
        logging.critical(tweet)

while True:
    extracted_tweets = extract()
    transformed_tweets = transform(extracted_tweets)
    load(transformed_tweets)
    time.sleep(30)
