from pyspark.sql.types import *
from pyspark.sql.functions import udf,col,lit,pandas_udf
# import nltk
# from nltk.sentiment import SentimentIntensityAnalyzer
# nltk.download('vader_lexicon')

# analyzer = SentimentIntensityAnalyzer()

def do_sentiment_analysis(df):
    # sentimentUdf = udf(get_sentiment,StringType())
    sentiment_udf = pandas_udf(get_sentiment, returnType=DoubleType())

    df_transformed = df \
        .withColumn("sentiment_score", sentiment_udf(df['text']))
    
    return df_transformed

# @pandas_udf(returnType=StringType())
def get_sentiment(text):
    if text is not None:
    # sentiment_score = analyzer.polarity_scores(text)
        return "abc" # sentiment_score['compound']

if __name__ == '__main__':
    do_sentiment_analysis()
