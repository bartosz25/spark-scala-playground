package com.waitingforcode.sql

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class RegressionTestsExampleTest extends FlatSpec with Matchers with BeforeAndAfter {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL JOIN-based regression test")
    .master("local[*]").getOrCreate()
  import sparkSession.implicits._

  private val referenceDataset = Seq(
    RegressionTestOrder(1L, 39.99d, Set(1L, 2L)),
    RegressionTestOrder(2L, 41.25d, Set(1L)),
    RegressionTestOrder(3L, 100d, Set(1L, 2L, 3L)),
    RegressionTestOrder(4L, 120d, Set(1L))
  ).toDF.cache() // cache it in order to avoid the recomputation

  private val generatedDataset = Seq(
    RegressionTestOrder(1L, 39.99d, Set(1L, 2L)),
    RegressionTestOrder(2L, 41.25d, Set.empty),
    RegressionTestOrder(3L, 200d, Set(1L, 2L, 3L)),
    RegressionTestOrder(100L, 200d, Set(1L, 2L, 3L))
  ).toDF.select($"id".as("generated_id"), $"amount".as("generated_amount"), $"itemIds".as("generated_itemIds")).cache()


  "LEFT JOIN with a custom comparator" should "be used to detect missing data" in {
    val allReferenceDataWithOptionalMatches =
      referenceDataset.join(generatedDataset, referenceDataset("id") === generatedDataset("generated_id"), "left")

    val notGeneratedReferenceData = allReferenceDataWithOptionalMatches.filter(row => row.getAs[Long]("generated_id") == null)
      .count()
    val commonEntries = allReferenceDataWithOptionalMatches.filter(row => row.getAs[Long]("generated_id") != null)
    commonEntries.cache()
    val invalidAmountGeneratedData = commonEntries
      .filter(row => row.getAs[Double]("generated_amount") != row.getAs[Double]("amount"))
    val invalidItemIdsGeneratedData = commonEntries
      .filter(row => row.getAs[Set[Long]]("generated_itemIds") != row.getAs[Set[Long]]("itemIds"))

    // Please notice that I'm using the .count() as an action but you can use any other valid action, like materializing
    // not matching data in order to investigate the inconsistencies later.
    notGeneratedReferenceData shouldEqual 1
    invalidAmountGeneratedData.count() shouldEqual 1
    invalidItemIdsGeneratedData.count() shouldEqual 1
  }

  "LEFT ANTI JOIN" should "be used to detect the data missing in the reference dataset" in {
    val extraGeneratedData = generatedDataset
      .join(referenceDataset, referenceDataset("id") === generatedDataset("generated_id"), "leftanti").count()

    extraGeneratedData shouldEqual 1
  }

  "FULL OUTER JOIN" should "be used to detect all errors with a single join" in {
    val allReferenceDataWithOptionalMatches =
      referenceDataset.join(generatedDataset, referenceDataset("id") === generatedDataset("generated_id"), "full_outer")

    val notGeneratedReferenceData = allReferenceDataWithOptionalMatches.filter(row => row.getAs[Long]("generated_id") == null)
      .count()
    val commonEntries = allReferenceDataWithOptionalMatches.filter(row => row.getAs[Long]("id") != null &&
      row.getAs[Long]("generated_id") != null)
    commonEntries.cache()
    val invalidAmountGeneratedData = commonEntries
      .filter(row => row.getAs[Double]("generated_amount") != row.getAs[Double]("amount"))
    val invalidItemIdsGeneratedData = commonEntries
      .filter(row => row.getAs[Set[Long]]("generated_itemIds") != row.getAs[Set[Long]]("itemIds"))
    val extraGeneratedData = allReferenceDataWithOptionalMatches.filter(row => row.getAs[Long]("generated_id") != null &&
      row.getAs[Long]("id") == null).count()

    notGeneratedReferenceData shouldEqual 1
    invalidAmountGeneratedData.count() shouldEqual 1
    invalidItemIdsGeneratedData.count() shouldEqual 1
    extraGeneratedData shouldEqual 1
  }

}


case class RegressionTestOrder(id: Long, amount: Double, itemIds: Set[Long])
