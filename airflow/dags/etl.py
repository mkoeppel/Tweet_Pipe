"""
1. Extract data from MongoDB
- Connect to the database
- Query the data

2. Transform the data
- Sentiment Analysis
- Transform data to load into postgres

3. Load it into Postgres
- Connect to postgres
- Insert Into postgres
"""

from datetime import datetime, timedelta
import logging

from pymongo import MongoClient
from sqlalchemy import create_engine
import psycopg2
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import config

POSTGRES_USER = config.POSTGRES_USER
POSTGRES_PASSWORD = config.POSTGRES_PASSWORD
POSTGRES_HOST = config.POSTGRES_HOST
POSTGRES_PORT = config.POSTGRES_PORT
POSTGRES_DB_NAME = config.POSTGRES_DB_NAME

conn_string = f"postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB_NAME}"
db_pg = create_engine(conn_string, echo=True)
#                   DB type    user     psw  host       port dbname
#db_pg = create_engine("postgres://postgres:1234@postgresdb:5432/postgres", echo=True)

create_table = """
                CREATE TABLE IF NOT EXISTS tweets (
                user_name TEXT,
                text TEXT,
                followers_count NUMERIC,
                location TEXT,
                reply_count NUMERIC,
                retweet_count NUMERIC,
                sentiment NUMERIC,
                keyword TEXT,
                timestamp TIMESTAMP
                );
               """

db_pg.execute(create_table)


def extract():
    """Extracts tweets from the MongoDB database
    with a time-interval spanning back to the previous extraction
    """
    client = MongoClient(host="mongodb", port=27017)
    # connect to the twitter database
    db_mongo = client.twitter
    # connect to the tweets collection
    tweets = db_mongo.twitter

    extraction_time = datetime.utcnow() - timedelta(minutes=1)
    extracted_tweets = list(tweets.find({"timestamp": {"$gte": extraction_time}}))
    return extracted_tweets


def transform(**context):
    """Transforms the data"""
    analyzer = SentimentIntensityAnalyzer()
    # connect with prior function 'extract'
    extract_connection = context["task_instance"]
    extracted_tweets = extract_connection.xcom_pull(task_ids="extract")

    transformed_tweets = []
    for tweet in extracted_tweets:
        sentiment = analyzer.polarity_scores(tweet["text"])
        tweet["sentiment"] = sentiment["compound"]
        transformed_tweets.append(tweet)
    return transformed_tweets


def load(**context):
    """Load transformed data into the postgres database"""
    # connect with prior function 'transform'
    transformed_tweets = context["task_instance"].xcom_pull(task_ids="transform")

    for tweet in transformed_tweets:
        insert_query = "INSERT INTO tweets VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        # DEFAULT,
        # :user_name,
        # :text,
        # :followers_count,
        # :location,
        # :reply_count,
        # :retweet_count,
        # :sentiment,
        # :keyword,
        # :timestamp)"""
        db_pg.execute(
            insert_query,
            (
                tweet["user_name"],
                tweet["text"],
                tweet["followers_count"],
                tweet["location"],
                tweet["reply_count"],
                tweet["retweet_count"],
                tweet["sentiment"],
                tweet["keyword"],
                tweet["timestamp"],
            ),
        )
        logging.critical("---Inserted a new tweet into postgres---")
        logging.critical(tweet)


# set default_args
default_args = {
    "owner": "maxn",
    "start_date": datetime(2020, 11, 25),
    "email": "[mmmaxwell7@gmail.com]",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# instantiate DAGs
dag = DAG(
    "tweet_analysis",
    description="An ETL pipeline",
    schedule_interval=timedelta(minutes=1),
    default_args=default_args,
    catchup=False,
)

# defined tasks
task_extract = PythonOperator(
    task_id="extract",
    python_callable=extract,
    dag=dag,
)
task_transform = PythonOperator(
    task_id="transform",
    provide_context=True,
    python_callable=transform,
    dag=dag,
)
task_load = PythonOperator(
    task_id="load",
    provide_context=True,
    python_callable=load,
    dag=dag,
)

task_extract >> task_transform >> task_load
