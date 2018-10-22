from __future__ import print_function

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def to_map(x):
    ids = x[1].split(",")
    map_list = []
    for id in ids:
        if x[0] < id:
            map_list.append([(x[0],id),list(x[1].split(","))])
        else:
            map_list.append([(id,x[0]),list(x[1].split(","))])
    return map_list


if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("PythonWordCount") \
        .getOrCreate()


    ds = spark.read.text('friends.txt')
    lines = ds.rdd.map(lambda x: x[0]).map(lambda x: x.split("\t")).filter(lambda x: len(x[1]) >0)
    rdd_map = lines.flatMap(lambda x: to_map(x))
    result = rdd_map.reduceByKey(lambda x,y : list(set(x)&set(y))).filter(lambda x: len(x[1]) > 0).map(lambda x: (x[0],len(x[1])))


    # # print(result.take(5))
    # # result.saveAsTextFile("./result/q1")
    # spark.stop()

    # spark sql
    df = spark.createDataFrame(result)
    df.show()


