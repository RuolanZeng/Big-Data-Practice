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
        .appName("PythonQ2") \
        .getOrCreate()

    # spark
    ds = spark.read.text('friends.txt')
    lines = ds.rdd.map(lambda x: x[0]).map(lambda x: x.split("\t")).filter(lambda x: len(x[1]) >0)
    rdd_map = lines.flatMap(lambda x: to_map(x))

    mutual_friends = rdd_map.reduceByKey(lambda x, y: list(set(x)&set(y)))
    mutual_friends_number = mutual_friends.map(lambda x: (x[0], len(x[1])))


    ds2 = spark.read.text('userdata.txt')
    ds2_lines = ds2.rdd.map(lambda x: x[0]).map(lambda x: (x.split(",")[0],(x.split(",")[1],x.split(",")[2],x.split(",")[3])))

    join1 = mutual_friends_number.map(lambda x: (x[0][0],(x[0][1],x[1]))).join(ds2_lines).map(lambda x: x[1])
    join2 = join1.map(lambda x: (x[0][0],(x[0][1],x[1]))).join(ds2_lines).map(lambda x: (x[1][0][0],x[1][0][1],x[1][1]))

    top_10 = join2.top(10, key=lambda x:x[0])
    top10_id = mutual_friends_number.top(10, key=lambda x:x[1])

    print(top_10)
    print(top10_id)








