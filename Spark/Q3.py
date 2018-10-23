from pyspark.sql import SparkSession
import pyspark.sql.functions as f


if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("PythonQ3") \
        .getOrCreate()

    # spark
    ds = spark.read.text('business.csv')
    business_rdd = ds.rdd.map(lambda x: x[0]).map(lambda x: x.split("::")).filter(lambda x: 'Stanford, CA' in x[1]).map(lambda x: (x[0],x[1])).distinct()
    # print(business_rdd.take(5))

    ds2 = spark.read.text('review.csv')
    review_rdd = ds2.rdd.map(lambda x: x[0]).map(lambda x: x.split("::")).map(lambda x: (x[2],(x[1],x[3])))
    # print(review_rdd.take(5))

    join_rdd = business_rdd.join(review_rdd).map(lambda x: x[1][1])
    print(join_rdd.count())

    # join_rdd.coalesce(1).saveAsTextFile("./result/q3")
    spark.stop()

    # spark sql
    # ds = spark.read.text('business.csv')
    # business_rdd = ds.rdd.map(lambda x: x[0]).map(lambda x: x.split("::"))
    # business_df = spark.createDataFrame(business_rdd)
    # # business_df.show()
    #
    # ds2 = spark.read.text('review.csv')
    # review_rdd = ds2.rdd.map(lambda x: x[0]).map(lambda x: x.split("::"))
    # review_df = spark.createDataFrame(review_rdd)
    # # review_df.show()
    #
    # business_df.registerTempTable("business")
    # review_df.registerTempTable("review")
    #
    # stanford = spark.sql("SELECT DISTINCT _1 AS business_id FROM  business "
    #                      "WHERE _2 LIKE '%Stanford, CA%'")
    #
    # # stanford.toPandas().to_csv("./result/test.csv", header=True)
    #
    # stanford.registerTempTable("stanford")
    #
    # result = spark.sql("SELECT review._2 AS User_id, review._4 AS Rating "
    #                    "FROM review INNER JOIN stanford "
    #                    "ON review._3 = stanford.business_id")
    #
    # result.toPandas().to_csv("./result/q3_sql.csv", header=True)



