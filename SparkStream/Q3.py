from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating


conf = SparkConf().setMaster("local").setAppName("Q2")
sc = SparkContext(conf=conf)
spark = SparkSession.\
    builder.\
    getOrCreate()

# Load and parse the data
data = sc.textFile("data/ratings.dat")
ratings = data.map(lambda l: l.split('::')).map(lambda l: Rating(int(l[0]), int(l[1]), int(l[2])))
splits = ratings.randomSplit([6, 4], 24)
train = splits[0]
test = splits[1]


# Build the recommendation model using Alternating Least Squares
rank = 10
numIterations = 10
model = ALS.train(train, rank, numIterations)

# Evaluate the model on training data
test_data = test.map(lambda p: (p[0], p[1]))
test_label = test.map(lambda r: ((r[0], r[1]), r[2]))
predictions = model.predictAll(test_data).map(lambda r: ((r[0], r[1]), r[2]))
ratesAndPreds = test_label.join(predictions)
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error = " + str(MSE))

