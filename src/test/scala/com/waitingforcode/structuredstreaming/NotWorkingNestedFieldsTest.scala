package com.waitingforcode.structuredstreaming

import java.sql.Timestamp

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class NotWorkingNestedFieldsTest extends FlatSpec with Matchers {

  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL nested data structures failures").master("local[*]").getOrCreate()
  import sparkSession.implicits._

  "watermark on a nested column" should "fail the query" in {
    val inputStream = new MemoryStream[LetterContainer](1, sparkSession.sqlContext)
    inputStream.addData(
      LetterContainer(TimeInformation(new Timestamp(0), "0"), "a"),
      LetterContainer(TimeInformation(new Timestamp(1), "1"), "b")
    )

    val treeError = intercept[Exception] {
      inputStream.toDS()
        .withWatermark("timeParams.watermark", "1 minute")
        .writeStream.format("console").option("truncate", "false")
        .start().awaitTermination(5000L)
    }

    treeError.getMessage() should include ("makeCopy, tree:")
    treeError.getMessage() should include ("EventTimeWatermark 'timeParams.watermark, interval 1 minutes")
  }

  "dropDuplicates on a nested column" should "fail the query" in {
    val inputStream = new MemoryStream[LetterContainer](1, sparkSession.sqlContext)
    inputStream.addData(
      LetterContainer(TimeInformation(new Timestamp(0), "0"), "a"),
      LetterContainer(TimeInformation(new Timestamp(1), "1"), "b")
    )

    val analysisException = intercept[AnalysisException] {
      inputStream.toDS()
        .dropDuplicates("timeParams.watermark", "1 minute")
        .writeStream.format("console").option("truncate", "false")
        .start().awaitTermination(5000L)
    }

    analysisException.getMessage() should startWith("Cannot resolve column name \"timeParams.watermark\" among (timeParams, letter);")
  }

  "extracted nested fields" should "be allowed in dropDuplicates and withWatermark" in {
    val inputStream = new MemoryStream[LetterContainer](1, sparkSession.sqlContext)
    inputStream.addData(
      LetterContainer(TimeInformation(new Timestamp(0), "0"), "a"),
      LetterContainer(TimeInformation(new Timestamp(1), "1"), "b")
    )

    inputStream.toDS()
      .select("timeParams.*")
      .withWatermark("watermark", "1 minute")
      .dropDuplicates("stringifiedTime")
      .writeStream.format("console").option("truncate", "false")
      .start().awaitTermination(5000L)
  }

  "analyzed plan output" should "only output first-class columns" in {
    val inputData = Seq(
      LetterContainer(TimeInformation(new Timestamp(0), "0"), "a"),
      LetterContainer(TimeInformation(new Timestamp(1), "1"), "b")
    ).toDS()

    val output = inputData.queryExecution.analyzed.output

    output.map(attribute => attribute.name) should contain only ("timeParams", "letter")
  }

  "partitionBy" should "also fail with a struct column" in {
    val inputData = Seq(
      LetterContainer(TimeInformation(new Timestamp(0), "0"), "a"),
      LetterContainer(TimeInformation(new Timestamp(1), "1"), "b")
    ).toDS()

    val analysisException = intercept[AnalysisException] {
      inputData.write.partitionBy("timeParams.watermark").json("/tmp/struct_in_partitionby")
    }

    analysisException.getMessage should startWith("Partition column `timeParams.watermark` not found in schema")
  }

}

case class LetterContainer(timeParams: TimeInformation, letter: String)
case class TimeInformation(watermark: Timestamp, stringifiedTime: String)