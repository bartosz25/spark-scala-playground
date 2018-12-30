package com.waitingforcode.sql.intersectexceptall

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class ExceptAllTest extends FlatSpec with Matchers {

  private val TestedSparkSession: SparkSession = SparkSession.builder()
    .appName("EXCEPT ALL test").master("local[*]").getOrCreate()
  import TestedSparkSession.implicits._
  val orders1 = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (3L, "user3"), (4L, "user1"), (5L, "user1"), (5L, "user1"))
    .toDF("order_id", "user_id")
  val orders2 = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (3L, "user3"), (4L, "user1"), (6L, "user1"))
    .toDF("order_id", "user_id")

  "EXCEPT" should "return all rows not present in the second dataset" in {
    val rowsFromDataset1NotInDataset2 = orders1.except(orders2)

    val exceptResult = rowsFromDataset1NotInDataset2.collect().map(row => row.getAs[Long]("order_id"))

    exceptResult should have size 1
    exceptResult(0) shouldEqual 5
    rowsFromDataset1NotInDataset2.explain(true)
  }

  "EXCEPT ALL" should "return all rows not present in the second dataset and keep the duplicates" in {
    val rowsFromDataset1NotInDataset2 = orders1.exceptAll(orders2)

    val exceptResultAll = rowsFromDataset1NotInDataset2.collect().map(row => row.getAs[Long]("order_id"))

    exceptResultAll should have size 2
    exceptResultAll(0) shouldEqual 5
    exceptResultAll(1) shouldEqual 5
    rowsFromDataset1NotInDataset2.explain(true)
  }
}
