from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors

conf = SparkConf().setMaster("local").setAppName("Q1")
sc = SparkContext(conf=conf)
spark = SparkSession.\
    builder.\
    getOrCreate()

lines = sc.textFile("data/itemusermat").map(lambda x: x.split(" ")).map(lambda x : [int(i) for i in x])

data = [(Vectors.dense(x),) for x in lines.collect()]
df = spark.createDataFrame(data, ["features"])

# train model
kmeans = KMeans(k=10, seed=1)
model = kmeans.fit(df)


# predict
predict_results = model.transform(df)

for i in range(10):
    cond = "prediction ==" + str(i)
    cluster1 = predict_results.filter(cond).limit(5)
    cluster1.show()
    ids = cluster1.rdd.map(lambda x: (str(int(x[0][0])), x[1]))
    # print("ids", ids.collect())

    movie_info = sc.textFile("data/movies.dat").map(lambda x: x.split("::")).map(lambda x: (x[0], (x[1], x[2])))
    # print("movie info:", movie_info.take(5))

    results = ids.join(movie_info).map(lambda x: [x[0], x[1][1][0], x[1][1][1], x[1][0]])
    path = "./result/q1/cluster" + str(i)
    # print(results.take(5))
    # print("path: ", path)
    results.coalesce(1).saveAsTextFile(path)


