package com.waitingforcode.sql

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class PartitionSortTest extends FlatSpec with Matchers {

  private val sparkSession = SparkSession.builder()
    .appName("Spark SQL dataset reduction")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  import sparkSession.implicits._

  "global ordering" should "range partition the data" in {
    val dataset = Seq((30), (40), (10), (20), (11), (25), (99), (109), (9), (2)).toDF("nr")

    val executionPlan = dataset.sort($"nr".desc).queryExecution.executedPlan

    executionPlan.toString() should include("Exchange rangepartitioning(nr#3 DESC NULLS LAST, 2)")
  }

  "local ordering" should "avoid shuffle exchange" in {
    val dataset = Seq((30), (40), (10), (20), (11), (25), (99), (109), (9), (2)).toDF("nr")

    val executionPlan = dataset.sortWithinPartitions($"nr".desc).queryExecution.executedPlan

    val executionPlanLevels = executionPlan.toString.split("\n")
    executionPlanLevels should have size 2
    executionPlanLevels(0) shouldEqual "*(1) Sort [nr#3 DESC NULLS LAST], false, 0"
    executionPlanLevels(1) shouldEqual "+- LocalTableScan [nr#3]"
  }

}
