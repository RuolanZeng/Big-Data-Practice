from kafka import KafkaProducer
import kafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# TWITTER API CONFIGURATIONS
consumer_key = "XmkOkViz0RULY41xUxS5Mw5na"
consumer_secret = "58SSqCbJasMMYj6yWhJpG3OqNuAmoymebqSznQQpsD8i31PN5j"
access_token = "1060592833101471744-EqIEYkseG20JDf7VUlr9OTJpGQtccL"
access_secret = "vsEAjvkc7uhKRKLK8k7JeKaqAsts1a4lWzufyWhOL4Vky"

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])



    def on_data(self, data):
        # Producer produces data for consumer

        self.producer.send("twitter", data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True


# Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

# Produce Data that has Game of Thrones hashtag (Tweets)
twitter_stream.filter(track=['#trump'])
