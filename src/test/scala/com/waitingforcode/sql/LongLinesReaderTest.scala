package com.waitingforcode.sql

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class LongLinesReaderTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val testInputFilePath = "/tmp/test-long-lines.txt"
  private val testInputFileShortDataPath = "/tmp/test-long-and_one-short-line.txt"

  override def beforeAll(): Unit = {
    val testData =
      """1AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz
        |2AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz
        |3AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz
        |4AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz
        |5AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz""".stripMargin
    FileUtils.writeStringToFile(new File(testInputFilePath), testData)
    val testDataWithShortText =
      s"""${testData}
         |1234
         |abcd""".stripMargin
    FileUtils.writeStringToFile(new File(testInputFileShortDataPath), testDataWithShortText)
  }

  override def afterAll(): Unit = {
    FileUtils.forceDelete(new File(testInputFilePath))
    FileUtils.forceDelete(new File(testInputFileShortDataPath))
  }

  "Hadoop line length configuration" should "be taken into account when reading the files" in {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Test too long lines")
      .config("spark.sql.files.maxPartitionBytes ", "5000000")
      .master("local[1]").getOrCreate()
    sparkSession.sparkContext.hadoopConfiguration.set("mapreduce.input.linerecordreader.line.maxlength", "20")

    val readText = sparkSession.read.textFile(testInputFilePath)
      .collect()

    readText shouldBe empty
  }

  "a line shorter than the limit" should "be returned at reading" in {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Test too long lines with one good line")
      .config("spark.sql.files.maxPartitionBytes ", "5000000")
      .master("local[1]").getOrCreate()
    sparkSession.sparkContext.hadoopConfiguration.set("mapreduce.input.linerecordreader.line.maxlength", "20")

    val readText = sparkSession.read.textFile(testInputFileShortDataPath)
      .collect()

    readText should have size 2
    readText should contain allOf("1234", "abcd")
  }
}