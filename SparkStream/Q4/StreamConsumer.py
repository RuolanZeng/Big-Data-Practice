from kafka import KafkaConsumer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import json
import string


def label(score):
    if score > 0.3:
        return "Positive"
    elif score < -0.2:
        return "Negative"
    else:
        return "Neutral"


def main():
    """
    Consumer consumes tweets from producer
    """
    # set-up a Kafka consumer
    consumer = KafkaConsumer('twitter')
    for msg in consumer:
        output = json.loads(msg.value)
        tweet_text = str(output['text'])
        tweet_token = word_tokenize(tweet_text)

        # print("text:", tweet_text)
        # print("tokens:", tweet_token)

        stopwords_set = set(stopwords.words("english")).union(set(string.punctuation))

        tokens_filtered = [word.lower() for word in tweet_token]

        tokens_cleaned = [word for word in tokens_filtered if
                                not word in stopwords_set and word != "RT" and "http" not in word
                                and not word.startswith('@')
                                and word != "rt"
                          ]

        #print("tokens_cleaned:", tokens_cleaned)

        # Sentiment analysis
        sia = SentimentIntensityAnalyzer()
        score = sia.polarity_scores(' '.join(tokens_cleaned))

        print("Tweet text: ", tweet_text)
        print("Scores:", score)
        print("Sentiment Analysis: ", label(score['compound']))
        print("\n")


if __name__ == "__main__":
    main()
