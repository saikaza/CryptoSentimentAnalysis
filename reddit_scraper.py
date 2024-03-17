import requests
import json
import csv
import datetime
import os

def fetch_reddit_data(query, start_time, end_time, subreddit):
    base_url = "https://api.pushshift.io/reddit/search/submission/"
    parameters = f"?title={query}&size=1000&after={start_time}&before={end_time}&subreddit={subreddit}"
    response = requests.get(f"{base_url}{parameters}")
    return json.loads(response.text)['data']

def process_submission_data(submission):
    submission_data = []
    submission_details = {
        'title': submission['title'],
        'url': submission['url'],
        'author': submission.get('author'),
        'id': submission['id'],
        'score': submission.get('score', 0),
        'created_utc': datetime.datetime.fromtimestamp(submission['created_utc']),
        'num_comments': submission.get('num_comments', 0),
        'permalink': submission['permalink'],
        'link_flair_text': submission.get('link_flair_text', 'NaN'),
        'selftext': submission.get('selftext', '').replace('[removed]', '').replace('[deleted]', '')
    }

    submission_data.append(submission_details)
    return submission_data

def save_data_to_csv(filename, submissions_data):
    headers = ['post_id', 'title', 'selftext', 'url', 'author', 'score', 'publish_date', 'num_of_comments', 'permalink', 'flair']
    write_mode = 'a' if os.path.exists(filename) else 'w'
    
    with open(filename, write_mode, newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers, delimiter=',')
        if file.tell() == 0:
            writer.writeheader()
        for submission in submissions_data:
            writer.writerow({
                'post_id': submission['id'],
                'title': submission['title'],
                'selftext': submission['selftext'],
                'url': submission['url'],
                'author': submission['author'],
                'score': submission['score'],
                'publish_date': submission['created_utc'],
                'num_of_comments': submission['num_comments'],
                'permalink': submission['permalink'],
                'flair': submission['link_flair_text']
            })

if __name__ == '__main__':
    subreddit = 'bitcoin'
    keyword = 'bitcoin'
    filename = 'reddit_data.csv'
    start_date = datetime.datetime(2018, 1, 1)
    end_date = datetime.datetime(2019, 11, 21)
    one_day = datetime.timedelta(days=1)

    current_date = start_date

    while current_date < end_date:
        print(f"Fetching data from: {current_date}")
        after = int(current_date.timestamp())
        before = int((current_date + one_day).timestamp())

        submissions = fetch_reddit_data(keyword, after, before, subreddit)
        processed_data = [process_submission_data(submission)[0] for submission in submissions]
        save_data_to_csv(filename, processed_data)

        current_date += one_day
        time.sleep(1) 
