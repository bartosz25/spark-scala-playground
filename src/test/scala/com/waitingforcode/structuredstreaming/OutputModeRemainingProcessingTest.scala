package com.waitingforcode.structuredstreaming

import com.waitingforcode.util.store.InMemoryKeyedStore
import com.waitingforcode.util.{InMemoryStoreWriter, NoopForeachWriter}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class OutputModeRemainingProcessingTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming output modes - remaining processing")
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  "append" should "work for map transform" in {
    val testKey = "other-processing-append-output-mode"
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val mappedResult = inputStream.toDS().toDF("id", "name")
      .map(row => row.getAs[String]("name"))
    inputStream.addData((1L, "test10"), (1L, "test11"), (2L, "test20"))

    val query = mappedResult.writeStream.outputMode("append")
      .foreach(new InMemoryStoreWriter[String](testKey, (mappedValue) => mappedValue)).start()
    new Thread(new Runnable() {
      override def run(): Unit = {
        while (!query.isActive) {}
        Thread.sleep(5000)
        inputStream.addData((1L, "test12"))
        inputStream.addData((1L, "test13"))
      }
    }).start()
    query.awaitTermination(30000)

    val processedValues = InMemoryKeyedStore.getValues(testKey)
    processedValues should have size 5
    processedValues should contain allOf("test10", "test11", "test12", "test13", "test20")

  }

  "update" should "work for map transform" in {
    val testKey = "other-processing-update-output-mode"
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val mappedResult = inputStream.toDS().toDF("id", "name")
      .map(row => row.getAs[String]("name"))
    inputStream.addData((1L, "test10"), (1L, "test11"), (2L, "test20"))

    val query = mappedResult.writeStream.outputMode("update")
      .foreach(new InMemoryStoreWriter[String](testKey, (mappedValue) => mappedValue)).start()
    new Thread(new Runnable() {
      override def run(): Unit = {
        while (!query.isActive) {}
        Thread.sleep(5000)
        inputStream.addData((1L, "test12"))
        inputStream.addData((1L, "test13"))
      }
    }).start()
    query.awaitTermination(30000)

    val processedValues = InMemoryKeyedStore.getValues(testKey)
    processedValues should have size 5
    processedValues should contain allOf("test10", "test11", "test12", "test13", "test20")
  }

  "complete mode" should "not be accepted in mapping transform" in {
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val mappedResults = inputStream.toDS().toDF("id", "name")
      .map(row => row.getAs[String]("name"))
    inputStream.addData((1L, "test1"), (1L, "test2"), (2L, "test3"))

    val exception = intercept[AnalysisException] {
      mappedResults.writeStream.outputMode("complete").foreach(new NoopForeachWriter[String]()).start()
    }

    exception.message should include("Complete output mode not supported when there are no streaming aggregations " +
      "on streaming DataFrames/Datasets")
  }

}
