package com.waitingforcode.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class CustomProjectionPushDownTest extends FlatSpec with Matchers with BeforeAndAfter {

  val sparkSession: SparkSession =  SparkSession.builder()
    .appName("Spark SQL job test").master("local[*]").getOrCreate()

  after {
    sparkSession.stop()
  }

  private val CommonJdbcOptions = Map("url" -> "jdbc:postgresql://localhost:5432/spark_test",
    "dbtable" -> "friends", "user" -> "root",
    "password" -> "root", "driver" -> "org.postgresql.Driver")

  private val ExpectedUsers = (1 to 40).map(nr => (s"user${nr}", s"Relation ${nr}"))

  "JSON column" should "be selected in 2 steps with from_json function" in {
    val allUsers = sparkSession.read
      .format("jdbc")
      .options(CommonJdbcOptions)
      .load()

    val jsonSchema = StructType(
      Seq(
        StructField("changes", IntegerType),
        StructField("fullName", StringType),
        StructField("important", BooleanType),
        StructField("businessValue", IntegerType)
      )
    )
    import sparkSession.implicits._
    // Since "friends" JSON column is retrieved as a StringType, we need some extra work to extract
    // its content. If you're interested why, take a look at [[org.apache.spark.sql.jdbc.PostgresDialect#toCatalystType]]
    // method where both JSON and JSONB types are considered as StringType:
    // ```
    // case "text" | "varchar" | "char" | "cidr" | "inet" | "json" | "jsonb" | "uuid" =>
    //      Some(StringType)
    // ```
    val usersWithExtractedFriendName = allUsers.select($"user_name", $"friends_list")
      // Please note that we could try to solve it more directly with
      // `.withColumn("fullName", explode($"friends_list.fullName"))` but it won't work because of this error:
      // ```
      // Can't extract value from friends_list#1: need struct type but got string;
      // ```
      .withColumn("friends_json", from_json($"friends_list", jsonSchema))
      .select($"user_name", $"friends_json.fullName")

    val localUsersWithFriend = usersWithExtractedFriendName.collectAsList()
    localUsersWithFriend should have size 40
    localUsersWithFriend should contain allElementsOf(ExpectedUsers)
  }

  "custom subquery in dbtable" should "extract only one JSON attribute" in {
    val jdbcOptions = CommonJdbcOptions ++ Map(
      "dbtable" -> "(SELECT user_name, friends_list->'fullName' AS friend_name, partition_nr FROM friends) AS friends_table",
      "partitionColumn" -> "partition_nr", "numPartitions" -> "2", "lowerBound" -> "1",
      "upperBound" -> "3")
    val allUsersWithFriend = sparkSession.read
      .format("jdbc")
      .options(jdbcOptions)
      .load()

    val localUsersWithFriend = allUsersWithFriend
      .collect().map(row => (row.getAs[String]("user_name"), row.getAs[String]("friend_name")))
    localUsersWithFriend should have size 40
    localUsersWithFriend should contain allElementsOf(ExpectedUsers)
  }

}
