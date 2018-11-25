from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import NaiveBayes



conf = SparkConf().setMaster("local").setAppName("Q2")
sc = SparkContext(conf=conf)
spark = SparkSession.\
    builder.\
    getOrCreate()

glass = sc.textFile("data/glass.data").map(lambda x: x.split(",")).map(lambda x : [float(i) for i in x])

# the last one is label, combine others as feature
data = glass.map(lambda x: (x[-1], Vectors.dense(x[:-1])))
df = spark.createDataFrame(data, ["label", "features"])
# use seed to insure, everytime run this program, the split is the same
splits = df.randomSplit([0.6, 0.4], seed=24) # seed=3, Decision Tree Accuaracy:  1.0, Naive Bayes Accuaracy:  0.1566265060240964
train = splits[0]
test = splits[1]
test_data = test.select("features")
test_label = test.select("label").collect()

# Decision Tree
dt = DecisionTreeClassifier(maxDepth=10, labelCol="label")
model = dt.fit(train)
prediction_result = model.transform(test_data).select("prediction").collect()

matched = 0
for i in range(len(prediction_result)):
    if test_label[i] == prediction_result[i]:
        matched += 1
acc = matched/len(prediction_result)

print("Decision Tree Accuaracy: ", acc)

# Naive Bayes
# Smoothing: Additive smoothing, default is 1.0. Model type: Multinomial (default) or Bernoulli.
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
model_nb = nb.fit(train)
prediction_nb = model_nb.transform(test_data).select("prediction").collect()

matched = 0
for i in range(len(prediction_nb)):
    if test_label[i] == prediction_nb[i]:
        matched += 1
acc_nb = matched/len(prediction_nb)

print("Naive Bayes Accuaracy: ", acc_nb)




