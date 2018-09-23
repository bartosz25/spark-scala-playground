package com.waitingforcode.sql.nested

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.{explode, explode_outer, posexplode}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

class NestedDataPredictableAndRepeatedTest extends FlatSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  private val sparkSession: SparkSession =  SparkSession.builder()
    .appName("Spark SQL nested repeated data test").master("local[*]").getOrCreate()
  import sparkSession.implicits._

  override def afterAll() {
    sparkSession.stop()
  }

  private val RepeatedNestedFile = "/tmp/spark/repeated_nested.json"

  before {
    val oneLevelEntries =
      """
        |{"user": "user_1", "orders": [{"id": "order#1", "amount": 39.5}, {"id": "order#2", "amount": 41.5}]}
        |{"user": "user_2", "orders": [{"id": "order#3", "amount": 112.5}]}
        |{"user": "user_3", "orders": []}
        |{"user": "user_4", "orders": [{"id": "order#4", "amount": 15.5}, {"id": "order#5", "amount": 21.5}, {"id": "order#6", "amount": 5.25}]}
        |{"user": "user_5", "orders": [{"id": "order#7", "amount": 31.33}, {"id": "order#8", "amount": 32}, {"id": "order#9", "amount": 10}, {"id": "order#10", "amount": 11}]}
        |{"user": "user_6", "orders": [{"id": "order#11", "amount": 15.11}]}
        |{"user": "user_7", "orders": [{"id": "order#12", "amount": 99}, {"id": "order#13", "amount": 45}]}
      """.stripMargin
    FileUtils.writeStringToFile(new File(RepeatedNestedFile), oneLevelEntries)
  }

  "DataFrame" should "repeated nested data with explode function" in {
    val oneLevelsDataFrame = sparkSession.read.json(RepeatedNestedFile)

    val ordersByUser = oneLevelsDataFrame.select($"user", explode($"orders").as("order"))

    val ordersByUserToCheck = ordersByUser.collect()
      .map(row => {
        val order = row.getAs[Row]("order")
        (row.getAs[String]("user"), order.getAs[String]("id"), order.getAs[Double]("amount"))
      })
    ordersByUserToCheck should have size 13
    ordersByUserToCheck should contain allOf(("user_1", "order#1", 39.5D), ("user_1", "order#2", 41.5D),
      ("user_2", "order#3", 112.5D), ("user_4", "order#4", 15.5D), ("user_4", "order#5", 21.5D), ("user_4", "order#6", 5.25D),
      ("user_5", "order#7", 31.33D), ("user_5", "order#8", 32.0D), ("user_5", "order#9", 10.0D), ("user_5", "order#10", 11.0D),
      ("user_6", "order#11", 15.11D), ("user_7", "order#12", 99.0D), ("user_7", "order#13", 45.0D))
  }

  "DataFrame" should "repeated nested data with explode_outer function" in {
    val oneLevelsDataFrame = sparkSession.read.json(RepeatedNestedFile)

    val ordersByUser = oneLevelsDataFrame.select($"user", explode_outer($"orders").as("order"))

    // explode_outer is very similar to explode
    // The difference is that it also takes empty arrays and maps and transforms them to null entries
    val ordersByUserToCheck = ordersByUser.collect()
      .map(row => {
        val order = row.getAs[Row]("order")
        if (order == null) {
          (row.getAs[String]("user"), null, null)
        } else {
          (row.getAs[String]("user"), order.getAs[String]("id"), order.getAs[Double]("amount"))
        }
      })
    ordersByUserToCheck should have size 14
    ordersByUserToCheck should contain allOf(("user_1", "order#1", 39.5D), ("user_1", "order#2", 41.5D),
      ("user_2", "order#3", 112.5D), ("user_4", "order#4", 15.5D), ("user_4", "order#5", 21.5D), ("user_4", "order#6", 5.25D),
      ("user_5", "order#7", 31.33D), ("user_5", "order#8", 32.0D), ("user_5", "order#9", 10.0D), ("user_5", "order#10", 11.0D),
      ("user_6", "order#11", 15.11D), ("user_7", "order#12", 99.0D), ("user_7", "order#13", 45.0D), ("user_3", null, null))
  }

  "DataFrame" should "repeated nested data with posexplode function" in {
    val oneLevelsDataFrame = sparkSession.read.json(RepeatedNestedFile)

    // posexplode behaves similar to explode except that it returns an extra colum called `position` indicating
    // the index of extracted data
    // It also has a variant for outer, exactly as explode, that helps to returns even rows without extracted element
    val ordersByUserWithOrderIndex = oneLevelsDataFrame.select($"user", posexplode($"orders"))

    val schema = ordersByUserWithOrderIndex.schema
    schema.simpleString shouldEqual "struct<user:string,pos:int,col:struct<amount:double,id:string>>"
    val ordersByUserWithOrderNrToCheck = ordersByUserWithOrderIndex.collect()
      .map(row => {
        val order = row.getAs[Row]("col")
        (row.getAs[String]("user"), row.getAs[Int]("pos"), order.getAs[String]("id"), order.getAs[Double]("amount"))
      })
    println(s"ordersByUserWithOrderNrToCheck=${ordersByUserWithOrderNrToCheck.mkString(",")}")
    ordersByUserWithOrderNrToCheck should have size 13
    ordersByUserWithOrderNrToCheck should contain allOf(("user_1", 0, "order#1", 39.5D), ("user_1", 1, "order#2", 41.5D),
      ("user_2", 0, "order#3", 112.5D), ("user_4", 0, "order#4", 15.5D), ("user_4", 1, "order#5", 21.5D), ("user_4", 2, "order#6", 5.25D),
      ("user_5", 0, "order#7", 31.33D), ("user_5", 1, "order#8", 32.0D), ("user_5", 2, "order#9", 10.0D), ("user_5", 3, "order#10", 11.0D),
      ("user_6", 0, "order#11", 15.11D), ("user_7", 0, "order#12", 99.0D), ("user_7", 1, "order#13", 45.0D))
  }

  "DataFrame" should "extract Map fields with explode function" in {
    // Explode also applies on Map type:
    val users = Seq(
      (1, Map("user" -> "user_1", "city" -> "London")),
      (2, Map("user" -> "user_2", "city" -> "Paris")),
      (3, Map("user" -> "user_3", "city" -> "Warsaw"))
    ).toDF("id", "user_info")

    val flattenedUsers = users.select($"id", explode($"user_info"))

    // explode applied on a Map will flatten the structure by extracting every key-value pair and put it beside
    // the root document. And extracted values will be called "key" for Map's key and "value" for its value
    flattenedUsers.schema.simpleString shouldEqual "struct<id:int,key:string,value:string>"
    val flattenedUsersMapped = flattenedUsers.collect().map(row => (row.getAs[Int]("id"), row.getAs[String]("key"), row.getAs[String]("value")))

    flattenedUsersMapped should have size 6
    flattenedUsersMapped should contain allOf((1, "user", "user_1"), (1, "city", "London"), (2, "user", "user_2"),
      (2, "city", "Paris"), (3, "user", "user_3"), (3, "city", "Warsaw"))
  }

}