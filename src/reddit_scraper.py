from datetime import datetime, timedelta
import logging
import praw
from kafka import KafkaProducer
import json
import yaml
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')

import os
config_file_path = os.path.abspath("../config/config.yaml")
with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# TODO: error handling

analyzer = SentimentIntensityAnalyzer()

def process_subreddit(producer:KafkaProducer, reddit_client:praw.Reddit, subject:str, 
                                      hours):
    end_date = datetime.utcnow()  # Current time
    start_date = end_date - timedelta(hours=hours)
    subreddit = reddit_client.subreddit(subject)
    # Retrieve comments from the subreddit
    all_comments = subreddit.comments()

    # Filter comments within the specified time range
    filtered_comments = [{
        "id": comment.id, 
        "text": comment.body,
        "creation_date": datetime.utcfromtimestamp(int(comment.created_utc)).strftime('%Y-%m-%d %H:%M:%S'),
        "subject": subject,
        "type": 'comment',
        'sentiment_score': analyzer.polarity_scores(comment.body)['compound']
    } for comment in all_comments if start_date <= datetime.utcfromtimestamp(comment.created_utc) <= end_date]

    print(filtered_comments)
    print(filtered_comments.__len__())

    send_messages_to_kafka(producer, filtered_comments, config['kafka']['comments_topic'])

    all_posts = subreddit.new(limit=100)
    filtered_posts = [{
        "id": post.id,
        "text": post.title,
        "creation_date": datetime.utcfromtimestamp(int(post.created_utc)).strftime('%Y-%m-%d %H:%M:%S'),
        "subject": subject,
        "type": 'post',
        'sentiment_score': None
    } for post in all_posts if start_date <= datetime.utcfromtimestamp(post.created_utc) <= end_date]

    print(filtered_posts)
    print(filtered_posts.__len__())

    send_messages_to_kafka(producer, filtered_posts, config['kafka']['posts_topic'])

def send_messages_to_kafka(producer, list, topic):
    for message in list:
        send_message_to_kafka(producer, message, topic)
    producer.flush()

def send_message_to_kafka(producer, message, topic):
    # logging.info('Sending messages to kafka...')
    producer.send(topic=topic, value=json.dumps(message).encode('utf-8')).get(timeout=10)

def get_kafka_producer():
    return KafkaProducer(bootstrap_servers=config['kafka']['server'])

def get_reddit_client():
    return praw.Reddit(client_id=config['reddit_api']['client_id'], client_secret=config['reddit_api']['client_secret'], 
                                user_agent=config['reddit_api']['user_agent'], check_for_async=False);

def main():
    start_time = datetime.now()
    logging.info(start_time.ctime() + ' - Starting job...')
    # 'reddit' object from praw (reddit_client API)
    reddit_client = get_reddit_client()
    # subreddit_clients to search
    subjects = ['powerbi', 'tableau', 'qlikview', 'looker', 'datastudio', 'domo', 'apachesuperset', 'lookerstudio']

    producer = get_kafka_producer();

    for subject in subjects:
        process_subreddit(producer, reddit_client, subject, 17)
        logging.info('Finished reading from ' + subject)

    end_time = datetime.now()
    processing_time = end_time - start_time

    producer.close()
    logging.info(end_time.ctime() + ' - Job ended. Process took ' + str(processing_time.seconds) + ' seconds.')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()