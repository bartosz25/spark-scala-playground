package com.waitingforcode.sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.scalatest.{FlatSpec, Matchers}

class BucketingTest extends FlatSpec with Matchers {

  private val TestedSparkSession: SparkSession = SparkSession.builder()
    .appName("Bucketing test").master("local[*]").getOrCreate()
  import TestedSparkSession.implicits._

  "Spark" should "create buckets in partitions for orders Dataset" in {
    val tableName = s"orders${System.currentTimeMillis()}"
    val orders = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (4L, "user1")).toDF("order_id", "user_id")

    orders.write.mode(SaveMode.Overwrite).bucketBy(2, "user_id").saveAsTable(tableName)

    val metadata = TestedSparkSession.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    metadata.bucketSpec shouldBe defined
    metadata.bucketSpec.get.numBuckets shouldEqual 2
    metadata.bucketSpec.get.bucketColumnNames(0) shouldEqual "user_id"
  }

  "Spark 2.4.0" should "not read buckets filtered out" in {
    val tableName = s"orders${System.currentTimeMillis()}"
    val orders = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (4L, "user1"), (5L, "user4"), (6L, "user5"))
      .toDF("order_id", "user_id")

    orders.write.mode(SaveMode.Overwrite).bucketBy(3, "user_id").saveAsTable(tableName)

    val filteredBuckets = TestedSparkSession.sql(s"SELECT * FROM ${tableName} WHERE user_id = 'user1'")

    filteredBuckets.queryExecution.executedPlan.toString() should include("SelectedBucketsCount: 1 out of 3")
  }

}

