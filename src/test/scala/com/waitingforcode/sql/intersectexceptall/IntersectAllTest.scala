package com.waitingforcode.sql.intersectexceptall

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class IntersectAllTest extends FlatSpec with Matchers {

  private val TestedSparkSession: SparkSession = SparkSession.builder()
    .appName("EXCEPT ALL test").master("local[*]").getOrCreate()
  import TestedSparkSession.implicits._
  val orders1 = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (3L, "user3"), (4L, "user1"), (5L, "user1"), (5L, "user1"))
    .toDF("order_id", "user_id")
  val orders2 = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (3L, "user3"), (4L, "user1"), (6L, "user1"))
    .toDF("order_id", "user_id")

  "INTERSECT" should "return all rows present in both datasets" in {
    val rowsFromDataset1InDataset2 = orders1.intersect(orders2)

    val intersectResult = rowsFromDataset1InDataset2.collect().map(row => row.getAs[Long]("order_id"))

    intersectResult should have size 4
    intersectResult should contain allOf(1L, 2L, 3L, 4L)
    rowsFromDataset1InDataset2.explain(true)
  }

  "INTERSECT ALL" should "return all rows present in both datasets and keeps the duplicates" in {
    val rowsFromDataset1InDataset2 = orders1.intersectAll(orders2)

    val intersectAllResult = rowsFromDataset1InDataset2.collect().map(row => row.getAs[Long]("order_id"))

    intersectAllResult should have size 5
    intersectAllResult should contain allElementsOf(Seq(1L, 2L, 3L, 3L, 4L))
    rowsFromDataset1InDataset2.explain(true)
  }
}
