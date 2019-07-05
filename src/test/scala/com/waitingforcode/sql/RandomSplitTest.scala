package com.waitingforcode.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class RandomSplitTest extends FlatSpec with Matchers {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("randomSplit test")
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  private def mapRowToInt(row: Row) = row.getAs[Int]("number")

  "seed" should "always return the same samples" in {
    val dataset = Seq(1, 2, 3, 4, 5).map(nr => nr).toDF("number")

    val splitsForSeed11Run1 = dataset.randomSplit(Array(0.1, 0.1), seed = 11L)
    val splitsForSeed11Run2 = dataset.randomSplit(Array(0.1, 0.1), seed = 11L)
    val splitsForSeed12Run1 = dataset.randomSplit(Array(0.1, 0.1), seed = 12L)

    val seed11Split0 = splitsForSeed11Run1(0).collect().map(mapRowToInt)
    val seed11Split1 = splitsForSeed11Run1(1).collect().map(mapRowToInt)
    splitsForSeed11Run2(0).collect().map(mapRowToInt) shouldEqual seed11Split0
    splitsForSeed11Run2(1).collect().map(mapRowToInt) shouldEqual seed11Split1
    splitsForSeed12Run1(0).collect().map(mapRowToInt) should not equal seed11Split0
    splitsForSeed12Run1(1).collect().map(mapRowToInt) should not equal seed11Split1
  }

  "randomSplit" should "return 2 splits with normalized weights" in {
    val dataset = (1 to 10).map(nr => nr).toDF("number")

    val splits = dataset.randomSplit(Array(0.1, 0.1), seed = 11L)

    val split1 = splits(0)
    split1.explain(true)
    val split1Data = split1.collect().map(mapRowToInt)
    split1Data should have size 6
    val split2 = splits(1)
    split2.explain(true)
    val split2Data = split2.collect().map(mapRowToInt)
    split2Data should have size 4
  }

}
