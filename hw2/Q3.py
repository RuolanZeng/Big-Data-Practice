from pyspark.sql import SparkSession
import pyspark.sql.functions as f


if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("PythonQ3") \
        .getOrCreate()

    # spark
    ds = spark.read.text('business.csv')
    business_rdd = ds.rdd.map(lambda x: x[0]).map(lambda x: x.split("::")).filter(lambda x: 'Stanford' in x[1]).map(lambda x: (x[0],x[1]))
    print(business_rdd.take(5))

    ds2 = spark.read.text('review.csv')
    review_rdd = ds2.rdd.map(lambda x: x[0]).map(lambda x: x.split("::")).map(lambda x: (x[2],(x[1],x[3])))
    print(review_rdd.take(5))

    join_rdd = business_rdd.join(review_rdd).map(lambda x: x[1][1])
    print(join_rdd.count())

    join_rdd.saveAsTextFile("./result/q3")
    spark.stop()

