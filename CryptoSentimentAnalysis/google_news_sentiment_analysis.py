import pandas as pd
import flair
from textblob import TextBlob
import os
import datetime
import numpy as np
from nltk.sentiment.vader import SentimentIntensityAnalyzer

DATE_FORMAT = '%Y-%m-%d'

def parse_flair_sentiment(sentiment):
    sentiment_str = str(sentiment)
    is_negative = 'NEGATIVE' in sentiment_str
    sentiment_value = sentiment_str.split('(')[-1].split(')')[0]
    return -float(sentiment_value) if is_negative else float(sentiment_value)

def update_dictionary(main_dict, updates):
    for key, value in updates.items():
        main_dict[key] = main_dict.get(key, 0) + value

def scale_dictionary_values(dictionary, scalar):
    for key in dictionary:
        dictionary[key] /= scalar

def analyze_sentiment(input_file, output_file, start_date=None):
    data_frame = pd.read_csv(input_file, index_col=0)
    if not start_date:
        sentiment_analyzer = flair.models.TextClassifier.load('en-sentiment')
        sid = SentimentIntensityAnalyzer()

    for index, row in data_frame.iterrows():
        if start_date:
            if datetime.datetime.strptime(index, DATE_FORMAT) < datetime.datetime.strptime(start_date, DATE_FORMAT):
                continue

        sentiments = {'flair': 0, 'tb_polarity': 0, 'tb_subjectivity': 0,
                      'sid_pos': 0, 'sid_neg': 0, 'sid_neu': 0, 'sid_compound': 0}
        total_counts = len(row.dropna())

        for content in row.dropna():
            tb_analysis = TextBlob(content).sentiment
            sentiments['tb_polarity'] += tb_analysis.polarity
            sentiments['tb_subjectivity'] += tb_analysis.subjectivity

            flair_sentence = flair.data.Sentence(content)
            sentiment_analyzer.predict(flair_sentence)
            sentiments['flair'] += parse_flair_sentiment(flair_sentence.labels)

            sid_scores = sid.polarity_scores(content)
            sentiments['sid_pos'] += sid_scores['pos']
            sentiments['sid_neg'] += sid_scores['neg']
            sentiments['sid_neu'] += sid_scores['neu']
            sentiments['sid_compound'] += sid_scores['compound']

        sentiments = {k: v / total_counts for k, v in sentiments.items()}
        sentiment_frame = pd.DataFrame([sentiments], index=[index])
        sentiment_frame.to_csv(output_file, mode='a', header=not os.path.exists(output_file))

def clean_sentiment_data(input_file, cleaned_file):
    df = pd.read_csv(input_file, index_col='date', parse_dates=True)
    df.sort_index(inplace=True)
    df.drop_duplicates(inplace=True)
    df.to_csv(cleaned_file)

if __name__ == "__main__":
    raw_file = 'google_news_final.csv'
    sentiment_file = raw_file.replace('.csv', '_sentiment.csv')
    analyze_sentiment(raw_file, sentiment_file)
    clean_sentiment_data(sentiment_file, sentiment_file.replace('.csv', '_cleaned.csv'))
