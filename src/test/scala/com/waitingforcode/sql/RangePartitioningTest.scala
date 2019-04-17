package com.waitingforcode.sql

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class RangePartitioningTest extends FlatSpec with BeforeAndAfter with Matchers {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL repartitionByRange")
    .config("spark.sql.shuffle.partitions", 5)
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  private val ordersToRepartition = Seq(
    (10, "order 1", 2000d), (11, "order 2", 240d),
    (12, "order 3", 232d), (13, "order 4", 100d),
    (14, "order 5", 11d), (15, "order 6", 20d),
    (16, "order 7", 390d), (17, "order 8", 30d),
    (18, "order 9", 99d), (19, "order 10", 55d),
    (20, "order 11", 129d), (21, "order 11", 75d),
    (22, "order 13", 173d)
  ).toDF("id", "name", "amount")

  "range partitioning" should "partition datasets in 3 partitions without explicit order" in {
    val repartitionedOrders = ordersToRepartition.repartitionByRange(3, $"id")
      .mapPartitions(rows => {
        val idsInPartition = rows.map(row => row.getAs[Int]("id")).toSeq.sorted.mkString(",")
        Iterator(idsInPartition)
      }).collect()

    repartitionedOrders should have size 3
    repartitionedOrders should contain allOf("10,11,12,13,14", "15,16,17,18", "19,20,21,22")
  }

  "range partitioning" should "partition datasets in 3 partitions with explicit order" in {
    val repartitionedOrders = ordersToRepartition.repartitionByRange(3, $"id".desc)
      .mapPartitions(rows => {
        val idsInPartition = rows.map(row => row.getAs[Int]("id")).toSeq.sorted.mkString(",")
        Iterator(idsInPartition)
      }).collect()

    ordersToRepartition.repartitionByRange(3, $"id".desc).explain(true)

    repartitionedOrders should have size 3
    repartitionedOrders should contain allOf("18,19,20,21,22", "14,15,16,17", "10,11,12,13")
  }

}