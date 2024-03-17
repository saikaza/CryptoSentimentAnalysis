import requests
from bs4 import BeautifulSoup
from newspaper import Article
import datetime
import pandas as pd
import time
import os
import numpy as np

GOOGLE_SEARCH_URL = "https://www.google.com/search?q=bitcoin+cryptocurrency&hl=en&gl=us&as_drrb=b&tbas=0&tbs=cdr:1,cd_min:{min_date},cd_max:{max_date},sbd:1&tbm=nws"

HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0',
    'Content-Type': 'text/html',
}

MAX_ARTICLES_PER_DAY = 10
DATE_FORMAT = '%m/%d/%Y'
COLUMNS_FOR_NEWS = [
    'index', 'date', 'status', 'search_url', 'article_1_url', 'article_1_content',
    'article_1_published', 'article_2_url', 'article_2_content', 'article_2_published',
]

def fetch_google_news(**kwargs):
    output_file = kwargs.get('output_file', '')
    min_date = kwargs.get('min_date', '')
    
    news_info = {'date': min_date}
    response = requests.get(GOOGLE_SEARCH_URL.format(**kwargs), headers=HTTP_HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')
    news_info['status'] = response.status_code
    if response.status_code != 200:
        return
    
    news_info['search_url'] = response.url
    article_count = 1
    for link in soup.find_all('a'):
        href = link.get('href')
        if href.startswith("https://") and "google.com" not in href:
            try:
                article = Article(href)
                article.download()
                article.parse()
                
                article_key = f'article_{article_count}'
                news_info[f'{article_key}_url'] = href
                news_info[f'{article_key}_content'] = article.text
                news_info[f'{article_key}_published'] = article.publish_date

                article_count += 1
                if article_count > MAX_ARTICLES_PER_DAY:
                    break
            except Exception:
                continue

    news_df = pd.DataFrame(news_info, index=[0], columns=COLUMNS_FOR_NEWS)
    news_df.to_csv(output_file, mode='a', header=not os.path.exists(output_file))

def scrape_google_news(start_date, end_date, file_name):
    current_date = datetime.datetime.strptime(start_date, DATE_FORMAT)
    end_date_obj = datetime.datetime.strptime(end_date, DATE_FORMAT)
    while current_date <= end_date_obj:
        formatted_date = current_date.strftime(DATE_FORMAT)
        fetch_google_news(min_date=formatted_date, max_date=formatted_date, output_file=file_name)
        time.sleep(np.random.randint(2, 5))
        current_date += datetime.timedelta(days=1)

if __name__ == "__main__":
    initial_date = '01/01/2024'
    final_date = '01/02/2024'
    raw_news_file = 'google_news_data.csv'

    scrape_google_news(initial_date, final_date, raw_news_file)
    cleaned_news_file = raw_news_file.replace('.csv', '_cleaned.csv')
    clean_news_report(raw_news_file, cleaned_news_file)
