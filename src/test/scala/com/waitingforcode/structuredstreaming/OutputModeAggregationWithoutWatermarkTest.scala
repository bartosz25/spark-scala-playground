package com.waitingforcode.structuredstreaming

import com.waitingforcode.util.{InMemoryStoreWriter, NoopForeachWriter}
import com.waitingforcode.util.store.InMemoryKeyedStore
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{AnalysisException, ForeachWriter, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class OutputModeAggregationWithoutWatermarkTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming output modes - aggregation with watermark")
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  "append mode without aggregation" should "be disallow in structured streaming without watermark" in {
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val counter =inputStream.toDS().toDF("id", "name").agg(count("*"))
    inputStream.addData((1L, "test1"), (1L, "test2"), (2L, "test3"))

    val exception = intercept[AnalysisException] {
      counter.writeStream.outputMode("append").foreach(new NoopForeachWriter()).start()
    }
    exception.message should include ("Append output mode not supported when there are streaming " +
      "aggregations on streaming DataFrames/DataSets without watermark")
  }

  "complete mode without aggregation" should "be allowed in structured streaming without watermark" in {
    val testKey = "no-aggregation-complete-output-mode"
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val counter =inputStream.toDS().toDF("id", "name").groupBy("name").agg(count("*"))
    inputStream.addData((1L, "test1"), (1L, "test2"), (2L, "test3"))
    inputStream.addData((1L, "test1"), (1L, "test2"), (2L, "test3"))
    inputStream.addData((1L, "test1"), (1L, "test2"), (2L, "test3"))

    val query = counter.writeStream.outputMode("complete").foreach(new ForeachWriter[Row] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(row: Row): Unit = {
        println(s"processing ${row}")
        InMemoryKeyedStore.addValue(testKey, s"${row.getAs[String]("name")}=${row.getAs[Long]("count(1)")}")
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }).start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        Thread.sleep(10000)
        // Add only 1  row to show that all entries are sent to the result table every time
        inputStream.addData((1L, "test1"))
      }
    }).start()

    query.awaitTermination(30000)

    val processedRows = InMemoryKeyedStore.getValues(testKey)
    processedRows should have size 6
    val rowsTest1 = processedRows.filter(_.startsWith("test1="))
    rowsTest1 should have size 2
    rowsTest1 should contain allOf("test1=3", "test1=4")
    val rowsTest2 = processedRows.filter(_.startsWith("test2="))
    rowsTest2 should have size 2
    rowsTest2 should contain only("test2=3")
    val rowsTest3 = processedRows.filter(_.startsWith("test3="))
    rowsTest3 should have size 2
    rowsTest3 should contain only("test3=3")
  }

  "update mode without aggregation" should "be allowed in structured streaming without watermark" in {
    val testKey = "no-aggregation-update-output-mode"
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val counter =inputStream.toDS().toDF("id", "name").groupBy("name").agg(count("*"))
    inputStream.addData((1L, "test1"), (1L, "test2"), (2L, "test3"))
    inputStream.addData((1L, "test1"), (1L, "test2"), (2L, "test3"))
    inputStream.addData((1L, "test1"), (1L, "test2"), (2L, "test3"))

    val query = counter.writeStream.outputMode("update").foreach(new InMemoryStoreWriter[Row](testKey,
      (processedRow) => s"${processedRow.getAs[String]("name")} -> ${processedRow.getAs[Long]("count(1)")}")).start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        Thread.sleep(10000)
        // Add only 1  row to show that only updated row is sent to the result table
        inputStream.addData((1L, "test1"))
      }
    }).start()

    query.awaitTermination(30000)

    val processedRows = InMemoryKeyedStore.getValues(testKey)
    processedRows should have size 4
    processedRows should contain allOf("test1 -> 3", "test1 -> 4", "test2 -> 3", "test3 -> 3")
  }
}
