from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def average(x):
    sum = 0
    count = len(x)
    for i in x:
        i = float(i)
        sum += i

    return sum/count



if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("PythonQ4") \
        .getOrCreate()

    # spark
    # ds = spark.read.text('business.csv')
    # business_rdd = ds.rdd.map(lambda x: x[0]).map(lambda x: x.split("::")).map(lambda x: (x[0],(x[1],x[2])))
    #
    # ds2 = spark.read.text('review.csv')
    # review_rdd = ds2.rdd.map(lambda x: x[0]).map(lambda x: x.split("::")).map(lambda x: (x[2],x[3]))
    #
    # join_rdd = business_rdd.join(review_rdd).map(lambda x: ((x[0],x[1][0][0],x[1][0][1]),x[1][1]))
    #
    #
    # results = join_rdd.groupByKey().map(lambda x: (x[0][0],x[0][1],x[0][2],average(x[1])))
    #
    # top_10 = results.top(10, key= lambda x: x[3])
    #
    # print(top_10)
    # spark.stop()

    # spark sql

    ds = spark.read.text('business.csv')
    business_rdd = ds.rdd.map(lambda x: x[0]).map(lambda x: x.split("::"))
    business_df = spark.createDataFrame(business_rdd)
    # business_df.show()

    ds2 = spark.read.text('review.csv')
    review_rdd = ds2.rdd.map(lambda x: x[0]).map(lambda x: x.split("::"))
    review_df = spark.createDataFrame(review_rdd)
    # review_df.show()

    business_df.registerTempTable("business")
    review_df.registerTempTable("review")

    join = spark.sql("SELECT business._1 AS business_id, business._2 AS full_address, business._3 AS categories, review._4 AS rating "
                     "FROM business,review "
                     "WHERE business._1 = review._3")

    join.registerTempTable("join")

    avg_rating = spark.sql("SELECT business_id, full_address, categories, AVG(rating) as avg_rating FROM join "
                           "GROUP BY business_id, full_address, categories"
                           )

    avg_rating.registerTempTable("avgRating")

    top_10 = spark.sql("SELECT * FROM avgRating ORDER BY avg_rating DESC LIMIT 10")
    top_10.show()



