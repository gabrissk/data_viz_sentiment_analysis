import praw
from kafka import KafkaProducer
import json

def process_comment(comment, new_post):
    print(comment.body)
    new_post['comments'].append(
        {
            "id": comment.id, 
            "text": comment.body, 
            "creation_date": comment.created_utc
        }
    )

    for reply in comment.replies:
        process_comment(reply, new_post)


def process_subreddit_client_comments(reddit_client:praw.Reddit, subject:str, posts:list):
    subreddit_client = reddit_client.subreddit(subject)
    for post in subreddit_client.new(limit=10):
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
        post.comments.replace_more()
        for comment in post.comments:
            process_comment(comment, new_post)
                
        # send_post_to_kafka(new_post)
        posts.append(new_post)


def send_post_to_kafka(posts:list):
    print("Sending to kafka...")
    with KafkaProducer(bootstrap_servers='localhost:9092') as producer:
        # post_dict = vars(posts)
        producer.send("reddit_client-dataviz-comments", json.dumps(posts).encode('utf-8')).get(timeout=10)
        producer.flush()


def main():
    # 'reddit' object from the praw  reddit_client API
    reddit_client = praw.Reddit(client_id='yQ83iAt0TWpfgiW07sTkOw', client_secret='q9na4iAgw25R8yBgLIDiNVaMwSp7EQ', user_agent='gabrissk', check_for_async=False)
    # subreddit_clients to search
    subjects = ["powerbi", "tableau", "qlikview", "looker", "datastudio", "domo"]

    posts = []

    for subject in subjects:
        process_subreddit_client_comments(reddit_client, subject, posts)
        print("Finished reading from ", subject)

    send_post_to_kafka(posts)


if __name__ == "__main__":
    main()