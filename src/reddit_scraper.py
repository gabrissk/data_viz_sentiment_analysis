import logging
import praw
from kafka import KafkaProducer
import json
import yaml

import os
config_file_path = os.path.abspath("../config/scraper_config.yaml")
with open('config/scraper_config.yaml', 'r') as f:
    config = yaml.safe_load(f)


def process_comment(comment, new_post):
    #print(comment.body)
    new_post['comments'].append(
        {
            "id": comment.id, 
            "text": comment.body, 
            "creation_date": comment.created_utc
        }
    )

    for reply in comment.replies:
        process_comment(reply, new_post)


def process_subreddit_client_comments(reddit_client:praw.Reddit, subject:str, posts:list, posts_limit:int=10):
    subreddit_client = reddit_client.subreddit(subject)
    for post in subreddit_client.new(limit=posts_limit):
        # print(post.title)
        # print(post.selftext)
        # print(post.created_utc)
        new_post = {
            "id": post.id, 
            "title": post.title, 
            "creation_date": post.created_utc,
            "subject": subject,
            "comments": []
        }
        # get full comments list, instead of reply references
        post.comments.replace_more()
        for comment in post.comments:
            process_comment(comment, new_post)
                
        # send_post_to_kafka(new_post)
        posts.append(new_post)


def send_post_to_kafka(posts:list):
    logging.info('Sending messages to kafka...')
    with KafkaProducer(bootstrap_servers=config['kafka']['server']) as producer:
        producer.send(config['kafka']['topic_name'], json.dumps(posts).encode('utf-8')).get(timeout=10)
        producer.flush()


def main():
    logging.info('Starting job...')
    # 'reddit' object from praw (reddit_client API)
    reddit_client = praw.Reddit(client_id=config['reddit_api']['client_id'], client_secret=config['reddit_api']['client_secret'], 
                                user_agent=config['reddit_api']['user_agent'], check_for_async=False)
    # subreddit_clients to search
    subjects = ['powerbi', 'tableau', 'qlikview', 'looker', 'datastudio', 'domo']

    posts = []

    for subject in subjects:
        process_subreddit_client_comments(reddit_client, subject, posts, 10)
        logging.info('Finished reading from ' + subject)

    send_post_to_kafka(posts)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()