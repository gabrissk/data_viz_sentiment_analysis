from datetime import datetime
import logging
import praw
from kafka import KafkaProducer
import json
import yaml

import os
config_file_path = os.path.abspath("../config/scraper_config.yaml")
with open('config/scraper_config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# TODO: error handling

def process_comment(comment, new_post):
    #print(comment.body)
    new_post['comments'].append(
        {
            "id": comment.id, 
            "text": comment.body, 
            "creation_date": datetime.utcfromtimestamp(int(comment.created_utc)).strftime('%Y-%m-%d %H:%M:%S')
        }
    )

    for reply in comment.replies:
        process_comment(reply, new_post)


def process_subreddit_client_comments(reddit_client:praw.Reddit, subject:str, posts:list, posts_limit:int=10):
    subreddit = reddit_client.subreddit(subject)
    for post in subreddit.new(limit=posts_limit):
        # print(post.title)
        # print(post.selftext)
        # print(post.created_utc)
        new_post = {
            "id": post.id, 
            "title": post.title, 
            "creation_date": datetime.utcfromtimestamp(int(post.created_utc)).strftime('%Y-%m-%d %H:%M:%S'),
            "subject": subject,
            "comments": []
        }

        if (post.comments.__len__() <= 0):
            new_post["comments"].append(
                {
                    "id": post.id + '-99999', 
                    "text": 'No Comments', 
                    "creation_date": datetime.utcfromtimestamp(int(post.created_utc)).strftime('%Y-%m-%d %H:%M:%S')
                }
            )
        # get full comments list, instead of reply references
        post.comments.replace_more()
        for comment in post.comments:
            process_comment(comment, new_post)
                
        send_post_to_kafka(new_post)
        posts.append(new_post)
        print(new_post)


def send_posts_to_kafka(posts:list):
    logging.info('Sending messages to kafka...')
    producer = KafkaProducer(bootstrap_servers=config['kafka']['server'])
    producer.send(config['kafka']['topic_name'], json.dumps(posts).encode('utf-8')).get(timeout=10)
    producer.flush()

def send_post_to_kafka(post):
    logging.info('Sending messages to kafka...')
    producer = KafkaProducer(bootstrap_servers=config['kafka']['server'])
    producer.send(topic=config['kafka']['topic_name'], value=json.dumps(post).encode('utf-8'), key=post["subject"].encode('utf-8')).get(timeout=10)
    producer.flush()
    producer.close()

def main():
    start_time = datetime.now()
    logging.info(start_time.ctime() + ' - Starting job...')
    # 'reddit' object from praw (reddit_client API)
    reddit_client = praw.Reddit(client_id=config['reddit_api']['client_id'], client_secret=config['reddit_api']['client_secret'], 
                                user_agent=config['reddit_api']['user_agent'], check_for_async=False)
    # subreddit_clients to search
    subjects = ['powerbi', 'tableau', 'qlikview', 'looker', 'datastudio', 'domo']

    posts = []

    for subject in subjects:
        process_subreddit_client_comments(reddit_client, subject, posts, 50)
        logging.info('Finished reading from ' + subject)

    # send_posts_to_kafka(posts)

    end_time = datetime.now()
    processing_time = end_time - start_time
    logging.info(end_time.ctime() + ' - Job ended. Process took ' + str(processing_time.seconds) + ' seconds.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()