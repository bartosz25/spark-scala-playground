package com.waitingforcode.tips

import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.{FlatSpec, Matchers}

class UdfFromColumnOperationTest extends FlatSpec with Matchers {

  "a UDF" should "be called in withColumn method" in {
    val testedSparkSession: SparkSession = SparkSession.builder()
      .appName("UDF from withColumn").master("local[*]").getOrCreate()
    import testedSparkSession.implicits._
    val orders = Seq((1L), (2L), (3L), (4L)).toDF("order_id")

    testedSparkSession.udf.register("generate_user_id", (orderId: Long) => s"user${orderId}")


    val ordersWithUserId =
      orders.withColumn("user_id", functions.callUDF("generate_user_id", $"order_id"))
      .map(row => (row.getAs[Long]("order_id"), row.getAs[String]("user_id")))
      .collect()

    ordersWithUserId should have size 4
    ordersWithUserId should contain allOf((1L, "user1"), (2L, "user2"), (3L, "user3"), (4L, "user4"))
  }

}

