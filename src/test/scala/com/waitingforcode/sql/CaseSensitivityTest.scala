package com.waitingforcode.sql

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CaseSensitivityTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val testInputFilePath = "/tmp/test-case-sensitivity.json"

  override def beforeAll(): Unit = {
    val differentCaseSensitivityData =
      """
        |{"key": "event_1", "value": 1 }
        |{"key": "event_2", "Value": 1 }
        |{"key": "event_3", "ValuE": 1  }
      """.stripMargin
    FileUtils.writeStringToFile(new File(testInputFilePath), differentCaseSensitivityData)
  }

  override def afterAll(): Unit = {
    FileUtils.forceDelete(new File(testInputFilePath))
  }

  "disabled case sensitivity" should "throw an exception for confusing schema" in {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("test")
      .config("spark.sql.caseSensitive", "false")
      .master("local[2]").getOrCreate()

    val schema = StructType(Seq(
      StructField("key", StringType), StructField("value", IntegerType)
      , StructField("Value", IntegerType)
    ))

    val error = intercept[AnalysisException] {
      sparkSession.read.schema(schema).json(testInputFilePath).show(false)
    }
    error.getMessage() shouldEqual "Found duplicate column(s) in the data schema: `value`;"
  }

  "enabled case sensitivity" should "get rows matching value and Value columns" in {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("test")
      .config("spark.sql.caseSensitive", "true")
      .master("local[2]").getOrCreate()
    import sparkSession.implicits._

    val schema = StructType(Seq(
      StructField("key", StringType), StructField("value", IntegerType)
      , StructField("Value", IntegerType)
    ))

    val rows = sparkSession.read.schema(schema).json(testInputFilePath)
      .filter(
        """
          |value = 1 OR Value = 1
        """.stripMargin)
      .map(row => (row.getAs[Int]("value"), row.getAs[Int]("Value")))
      .collect()

    rows should have size 2
    rows should contain allOf((0, 1), (1, 0))
  }

  "schema sensitivity" should "be respected even with case sensitive flag disabled" in {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("test")
      .config("spark.sql.caseSensitive", "false")
      .master("local[2]").getOrCreate()
    import sparkSession.implicits._

    val schema = StructType(Seq(
      StructField("key", StringType), StructField("Value", IntegerType)
    ))

    val rows = sparkSession.read.schema(schema).json(testInputFilePath)
      .filter(
        """
          |Value = 1
        """.stripMargin)
      .map(row => (row.getAs[String]("key"), row.getAs[Int]("Value")))
      .collect()

    rows should have size 1
    rows(0) shouldEqual ("event_2", 1)
    val schemaFromQuery = sparkSession.read.schema(schema).json(testInputFilePath).schema.toString
    schemaFromQuery shouldEqual "StructType(StructField(key,StringType,true)," +
      " StructField(Value,IntegerType,true))"
  }

  "case sensitivity disabled" should "return only 1 row with filter" in {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("test")
      .config("spark.sql.caseSensitive", "false")
      .master("local[2]").getOrCreate()
    import sparkSession.implicits._

    val schema = StructType(Seq(
      StructField("key", StringType), StructField("value", IntegerType)
    ))

    val rows = sparkSession.read.schema(schema).json(testInputFilePath)
      .filter(
        """
          |value = 1 OR Value = 1
        """.stripMargin)
      // cannot retrieve "Value" because of:
      // IllegalArgumentException: Field "Value" does not exist.
      // Available fields: key, value
      .map(row => (row.getAs[String]("key"), row.getAs[Int]("value")))
      .collect()

    rows should have size 1
    rows(0) shouldEqual ("event_1", 1)
  }

}