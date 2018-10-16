from pyspark import SparkContext
from pyspark import SparkConf
from operator import add

#List the business_id of the Top 10 mostly-reviewed businesses based on number of users that have rated these businesses.

conf = SparkConf().setMaster("local").setAppName("sample")
sc = SparkContext(conf=conf)

#data is in the same folder of this script
#load data
review = sc.textFile("review.csv").map(lambda line: line.split("::"))


# # calculate number of reviews
review_by_business = review.map(lambda x: (x[2], 1)).reduceByKey(add)
print(review_by_business.take(5))

top_10 = review_by_business.top(10, key=lambda x: x[1])
print(top_10)
