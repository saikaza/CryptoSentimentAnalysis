import pandas as pd
import flair
from textblob import TextBlob
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import datetime
import numpy as np
import os

flair_sentiment_model = flair.models.TextClassifier.load('en-sentiment')
sentiment_intensity_analyzer = SentimentIntensityAnalyzer()
DATE_FORMAT = '%Y-%m-%d %H:00:00'

def parse_flair_sentiment(sentiment):
    sentiment_str = str(sentiment)
    is_negative = 'NEGATIVE' in sentiment_str
    sentiment_value = float(sentiment_str.split(' ')[-1].strip(')').strip(']'))
    return -sentiment_value if is_negative else sentiment_value

def analyze_sentiment(input_file, output_file):
    data = pd.read_csv(input_file)
    data.fillna('', inplace=True)
    data['text'] = data['title'] + ' ' + data['selftext']
    data['publish_date'] = pd.to_datetime(data['publish_date']).dt.strftime(DATE_FORMAT)
    data.set_index('publish_date', inplace=True)

    sentiments = pd.DataFrame(index=data.index)
    for index, row in data.iterrows():
        text = row['text']
        flair_sentence = flair.data.Sentence(text)
        flair_sentiment_model.predict(flair_sentence)
        flair_sentiment = parse_flair_sentiment(flair_sentence.labels)

        textblob_sentiment = TextBlob(text).sentiment
        sid_scores = sentiment_intensity_analyzer.polarity_scores(text)

        sentiments.at[index, 'flair_sentiment'] = flair_sentiment
        sentiments.at[index, 'tb_polarity'] = textblob_sentiment.polarity
        sentiments.at[index, 'tb_subjectivity'] = textblob_sentiment.subjectivity
        sentiments.at[index, 'sid_pos'] = sid_scores['pos']
        sentiments.at[index, 'sid_neg'] = sid_scores['neg']
        sentiments.at[index, 'sid_neu'] = sid_scores['neu']
        sentiments.at[index, 'sid_compound'] = sid_scores['compound']

    sentiments.reset_index().to_csv(output_file, index=False)

def bucketize_sentiment_data(input_file, output_file):
    df = pd.read_csv(input_file)
    df['publish_date'] = pd.to_datetime(df['publish_date'])
    df.set_index('publish_date', inplace=True)

    resampled = df.resample('H').mean().reset_index()
    resampled['publish_date'] = resampled['publish_date'].dt.strftime(DATE_FORMAT)

    resampled.to_csv(output_file, index=False)

if __name__ == '__main__':
    input_filename = 'reddit_data.csv'
    sentiment_output_filename = input_filename.replace('.csv', '_sentiment.csv')

    analyze_sentiment(input_filename, sentiment_output_filename)

    bucketized_output_filename = sentiment_output_filename.replace('.csv', '_bucketized.csv')
    bucketize_sentiment_data(sentiment_output_filename, bucketized_output_filename)
