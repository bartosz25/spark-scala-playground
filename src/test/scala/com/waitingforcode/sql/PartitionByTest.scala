package com.waitingforcode.sql

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class PartitionByTest extends FlatSpec with Matchers {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL partitionBy")
    .config("spark.sql.shuffle.partitions", "1")
    .master("local[1]").getOrCreate()
  import sparkSession.implicits._

  "partitionBy" should "not write partition columns in the dataset" in {
    val outputDir = "/tmp/partitionby_test"

    val rawNumbers = Seq(
      (1, "a"), (2, "aa"), (1, "b"), (3, "c"), (1, "c"), (4, "c"), (1, "a"),
      (5, "de"), (6, "e"), (1, "a"), (1, "a"), (1, "a"), (1, "a"), (2, "aa"),
      (2, "aa"), (2, "de")
    )

    rawNumbers.toDF("id", "word")
      .write
      .partitionBy("word")
      .mode(SaveMode.Overwrite).json(outputDir)

    val matchingFiles = FileUtils.listFiles(new File(outputDir), Array("json"), true)
    import scala.collection.JavaConverters._
    val writtenContent = matchingFiles.asScala.map(file => {
      FileUtils.readFileToString(file)
    })
    writtenContent.mkString("\n") should not include "word"

    val savedDataset = sparkSession.read.json(outputDir).select("word").as[String]
      .collect()
    savedDataset should contain allElementsOf(
      rawNumbers.map(idWithWord => idWithWord._2)
      )
  }

  "partitionBy" should "fail when the partitions don't match the dataset schema" in {
    val outputDir = "/tmp/partitionby_test_not_matching_schema"

    val rawNumbers = Seq(
      (1, "a"), (2, "aa"), (1, "b"), (3, "c"), (1, "c"), (4, "c"), (1, "a"),
      (5, "de"), (6, "e"), (1, "a"), (1, "a"), (1, "a"), (1, "a"), (2, "aa"),
      (2, "aa"), (2, "de")
    ).toDF("id", "word")

    val exception = the [AnalysisException] thrownBy  {
      rawNumbers.write
        .partitionBy("number_id")
        .mode(SaveMode.Overwrite).json(outputDir)
    }
    exception.getMessage() should startWith("Partition column `number_id` not found in " +
      "schema struct<id:int,word:string>;")
  }

  "partitionBy" should "allow to use different partition columns in the same physical location" in {
    val outputDir = "/tmp/partitionby_test_different_columns"

    val rawNumbers = Seq(
      (1, "a"), (2, "aa"), (1, "b"), (3, "c"), (1, "c"), (4, "c"), (1, "a"),
      (5, "de"), (6, "e"), (1, "a"), (1, "a"), (1, "a"), (1, "a"), (2, "aa"),
      (2, "aa"), (2, "de")
    ).toDF("id", "word")

    rawNumbers.write
      .partitionBy("id")
      .mode(SaveMode.Overwrite).json(outputDir)

    rawNumbers.write
      .partitionBy("word")
      .mode(SaveMode.Append).json(outputDir)


    val matchingFiles = FileUtils.listFiles(new File(outputDir), Array("json"), true)
    import scala.collection.JavaConverters._
    val generatedFiles = matchingFiles.asScala.map(file => {
      file.getAbsolutePath
    })
    val expectedPartitions = Seq("id=1", "id=2", "id=3", "id=4", "id=5", "id=6",
      "word=a", "word=aa", "word=b", "word=c", "word=de", "word=e")
    expectedPartitions.foreach(partition => generatedFiles.mkString("\n") should include(partition))
  }

}
