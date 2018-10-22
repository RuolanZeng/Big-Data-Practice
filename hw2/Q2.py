from pyspark.sql import SparkSession
import pyspark.sql.functions as f


def to_map(x):
    ids = x[1].split(",")
    map_list = []
    for id in ids:
        if x[0] < id:
            map_list.append([(x[0], id), list(x[1].split(","))])
        else:
            map_list.append([(id, x[0]), list(x[1].split(","))])
    return map_list


if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("PythonQ2") \
        .getOrCreate()

    # spark
    # ds = spark.read.text('friends.txt')
    # lines = ds.rdd.map(lambda x: x[0]).map(lambda x: x.split("\t")).filter(lambda x: len(x[1]) >0)
    # rdd_map = lines.flatMap(lambda x: to_map(x))
    #
    # mutual_friends = rdd_map.reduceByKey(lambda x, y: list(set(x)&set(y)))
    # mutual_friends_number = mutual_friends.map(lambda x: (x[0], len(x[1])))
    #
    # ds2 = spark.read.text('userdata.txt')
    # ds2_lines = ds2.rdd.map(lambda x: x[0]).map(lambda x: (x.split(",")[0],(x.split(",")[1],x.split(",")[2],x.split(",")[3])))
    #
    # join1 = mutual_friends_number.map(lambda x: (x[0][0],(x[0][1],x[1]))).join(ds2_lines).map(lambda x: x[1])
    # join2 = join1.map(lambda x: (x[0][0],(x[0][1],x[1]))).join(ds2_lines).map(lambda x: (x[1][0][0],x[1][0][1],x[1][1]))
    #
    # top_10 = join2.top(10, key=lambda x:x[0])
    # print(top_10)

    # spark sql

    # create dataframe
    ds = spark.read.text('friends.txt')
    lines = ds.rdd.map(lambda x: x[0]).map(lambda x: x.split("\t")).filter(lambda x: len(x[1]) > 0)
    rdd_map = lines.flatMap(lambda x: to_map(x))
    mutual_friends_number = rdd_map.reduceByKey(lambda x, y: list(set(x) & set(y))) \
        .filter(lambda x: len(x[1]) > 0) \
        .map(lambda x: (x[0][0], x[0][1], len(x[1])))

    mutual_friends_df = spark.createDataFrame(mutual_friends_number)

    ds2 = spark.read.text('userdata.txt')
    ds2_lines = ds2.rdd.map(lambda x: x[0]).map(lambda x: x.split(","))
    userdata_df = spark.createDataFrame(ds2_lines)

    # create table
    mutual_friends_df.registerTempTable("MutualFriends")
    userdata_df.registerTempTable("UserData")

    # mutual_friends_df.show()
    # userdata_df.show()

    top_10 = spark.sql("SELECT * FROM MutualFriends ORDER BY _3 DESC LIMIT 10")
    top_10.registerTempTable("Top10")

    join1 = spark.sql("SELECT Top10._3 AS MutualFriendsNumber, Top10._1 AS user1, UserData._2 AS user1First, "
                      "UserData._3 AS user1Last, UserData._4 AS user1Address, Top10._2 AS user2 "
                      "FROM  Top10, UserData "
                      "WHERE Top10._1 = UserData._1")

    # join1.show()
    join1.registerTempTable("join1")

    join2 = spark.sql("SELECT join1.*, UserData._2 AS user2First, UserData._3 AS user2Last, UserData._4 AS user2Address "
                      "FROM join1,  UserData "
                      "WHERE join1.user2 = UserData._1")

    join2.show()


