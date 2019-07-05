package com.waitingforcode.sql

import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.{FlatSpec, Matchers}

class SkewedJoinSaltingTest extends FlatSpec with Matchers {

  private val TestedSparkSession: SparkSession = SparkSession.builder()
    .appName("Skewed join salting test").master("local[*]").getOrCreate()
  import TestedSparkSession.implicits._

  "Spark" should "create buckets in partitions for orders Dataset" in {
    val users = Seq(("user1"), ("user2"), ("user3")).toDF("id")
    val orders = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (4L, "user1"), (5L, "user1")
      , (6L, "user1"), (7L, "user1")).toDF("order_id", "user_id")

    val ordersWithSaltedCol = orders.withColumn("order_join_key", functions.concat($"user_id",
      functions.floor(functions.rand(2) * 2))
    )

    val usersWithSaltedCol = users.withColumn("salt", functions.array(functions.lit(0), functions.lit(1), functions.lit(2)))
      .withColumn("user_salt", functions.explode($"salt"))
      .withColumn("user_join_key", functions.concat($"id", $"user_salt"))


    val result = usersWithSaltedCol.join(ordersWithSaltedCol, $"user_join_key" === $"order_join_key")

    val mappedUsers = result.collect().map(row => s"${row.getAs[String]("id")}_${row.getAs[Int]("order_id")}")
    mappedUsers should have size 7
    mappedUsers should contain allOf("user1_1", "user2_2", "user3_3", "user1_4", "user1_5", "user1_6", "user1_7")
    result.show(true)
  }

}
