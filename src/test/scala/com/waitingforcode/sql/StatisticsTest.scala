package com.waitingforcode.sql

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class StatisticsTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder().appName("Statistics test").master("local")
      .getOrCreate()

  import sparkSession.implicits._
  val shops = Seq(
    (1, "Shop_1"), (2, "Shop_2"), (3, "Shop_3"), (4, "Shop_4"), (5, "Shop_5")
  ).toDF("id", "name")
  val revenues = Seq(
    (1, 1000), (2, 2000), (3, 3000), (4, 4000), (5, 5000)
  ).toDF("id", "revenue")

  override def afterAll(): Unit = {
    sparkSession.stop()
  }

  "statistics" should "remain unchanged for simple filter operation" in {
    val pairIdShops = shops.filter(shops("id")%2 === 0)

    val queryExecution = pairIdShops.queryExecution.stringWithStats

    // Expected statistics:
    // == Optimized Logical Plan ==
    // LocalRelation [id#5, name#6], Statistics(sizeInBytes=64.0 B, hints=none)
    //
    // == Physical Plan ==
    //   LocalTableScan [id#5, name#6]" did not include substring "Statistics(sizeInBytes=120.0 B, hints=none)"
    // As told in the post, the size doesn't change because the projection remains the same
    queryExecution should include("Statistics(sizeInBytes=120.0 B, hints=none)")
    val queryExecutionWithoutFilter = shops.queryExecution.stringWithStats
    queryExecutionWithoutFilter should include("Statistics(sizeInBytes=120.0 B, hints=none)")
  }

  "statistics" should "change after joining 2 datasets" in {
    val shopsWithRevenues = shops.join(revenues)

    val queryExecution = shopsWithRevenues.queryExecution.stringWithStats

    // Expected plan is:
    // Join Inner, Statistics(sizeInBytes=4.7 KB, hints=none)
    // :- LocalRelation [id#25, name#26], Statistics(sizeInBytes=120.0 B, hints=none)
    // +- LocalRelation [id#35, revenue#36], Statistics(sizeInBytes=40.0 B, hints=none)
    queryExecution should include("Join Inner, Statistics(sizeInBytes=4.7 KB, hints=none)")
    queryExecution should include("Statistics(sizeInBytes=120.0 B, hints=none)")
    queryExecution should include("Statistics(sizeInBytes=40.0 B, hints=none)")
  }

  "statistics for limit" should "decrease" in {
    val limitedShops = shops.limit(2)

    val queryExecution = limitedShops.queryExecution.stringWithStats

    // Expected plan is:
    // == Optimized Logical Plan ==
    // GlobalLimit 2, Statistics(sizeInBytes=64.0 B, rowCount=2, hints=none)
    // +- LocalLimit 2, Statistics(sizeInBytes=120.0 B, hints=none)
    // +- LocalRelation [id#45, name#46], Statistics(sizeInBytes=120.0 B, hints=none)
    queryExecution should include("GlobalLimit 2, Statistics(sizeInBytes=64.0 B, rowCount=2, hints=none)")
    queryExecution should include("Statistics(sizeInBytes=120.0 B, hints=none)")
  }

  "statistics" should "increase when using union operator" in {
    val newShops = Seq(
      (11, "Shop_11"), (12, "Shop_12"), (13, "Shop_13"), (14, "Shop_14"), (15, "Shop_15")
    ).toDF("id", "Name")
    val allShops = shops.union(newShops)

    val queryExecution = allShops.queryExecution.stringWithStats

    // Expected plan is:
    // == Optimized Logical Plan ==
    // Union, Statistics(sizeInBytes=240.0 B, hints=none)
    // :- LocalRelation [id#65, name#66], Statistics(sizeInBytes=120.0 B, hints=none)
    // +- LocalRelation [id#91, Name#92], Statistics(sizeInBytes=120.0 B, hints=none)
    queryExecution should include("Union, Statistics(sizeInBytes=240.0 B, hints=none)")
    queryExecution should include("Statistics(sizeInBytes=120.0 B, hints=none)")
  }

}
