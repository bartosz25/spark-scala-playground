package com.waitingforcode.structuredstreaming

import java.sql.Timestamp

import com.waitingforcode.util.store.InMemoryKeyedStore
import com.waitingforcode.util.{InMemoryStoreWriter, NoopForeachWriter}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class OutputModeAggregationWithWatermarkTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming output modes - with watermark")
    .master("local[2]").getOrCreate()
  import sparkSession.sqlContext.implicits._

  "the count on watermark column" should "be correctly computed in append mode" in {
    val testKey = "append-output-mode-with-watermark-aggregation"
    val inputStream = new MemoryStream[(Timestamp, Int)](1, sparkSession.sqlContext)
    val now = 5000L
    val aggregatedStream = inputStream.toDS().toDF("created", "number")
      .withWatermark("created", "1 second")
      .groupBy("created")
      .count()

    val query = aggregatedStream.writeStream.outputMode("append")
      .foreach(new InMemoryStoreWriter[Row](testKey,
        (processedRow) => s"${processedRow.getAs[Long]("created")} -> ${processedRow.getAs[Long]("count")}"))
      .start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        // send the first batch - max time = 10 seconds, so the watermark will be 9 seconds
        inputStream.addData((new Timestamp(now+5000), 1), (new Timestamp(now+5000), 2), (new Timestamp(now+4000), 3),
          (new Timestamp(now+5000), 4))
        while (!query.isActive) {}
        Thread.sleep(10000)
        inputStream.addData((new Timestamp(4000L), 5), (new Timestamp(now+4000), 6))
        inputStream.addData((new Timestamp(11000), 7))
        Thread.sleep(10000)
        inputStream.addData((new Timestamp(11000), 8))
      }
    }).start()

    query.awaitTermination(45000)

    val readValues = InMemoryKeyedStore.getValues(testKey)
    // As you can see, only the result for now+5000 was emitted. It's because the append output
    // mode emits the results once a new watermark is defined and the accumulated results are below
    // the new threshold
    readValues should have size 2
    readValues should contain allOf("1970-01-01 01:00:09.0 -> 2", "1970-01-01 01:00:10.0 -> 3")
  }

  "the count on non-watermark column" should "fail in append mode" in {
    val inputStream = new MemoryStream[(Timestamp, Int)](1, sparkSession.sqlContext)
    val aggregatedStream = inputStream.toDS().toDF("created", "number")
      .withWatermark("created", "1 second")
      .groupBy("number")
      .count()

    val exception = intercept[AnalysisException]{
      aggregatedStream.writeStream.outputMode("append")
        .foreach(new NoopForeachWriter[Row]()).start()
    }

    exception.message should include("Append output mode not supported when there are streaming aggregations on " +
      "streaming DataFrames/DataSets without watermark")
  }

  "the count on watermark column" should "be correctly computed in update mode" in {
    val testKey = "update-output-mode-with-watermark-aggregation"
    val inputStream = new MemoryStream[(Timestamp, Int)](1, sparkSession.sqlContext)
    val now = 5000L
    val aggregatedStream = inputStream.toDS().toDF("created", "number")
      .withWatermark("created", "1 second")
      .groupBy("created")
      .count()

    val query = aggregatedStream.writeStream.outputMode("update")
      .foreach(new InMemoryStoreWriter[Row](testKey,
        (processedRow) => s"${processedRow.getAs[Long]("created")} -> ${processedRow.getAs[Long]("count")}")).start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        inputStream.addData((new Timestamp(now+5000), 1), (new Timestamp(now+5000), 2), (new Timestamp(now+5000), 3),
          (new Timestamp(now+5000), 4))
        while (!query.isActive) {}
        Thread.sleep(10000)
        inputStream.addData((new Timestamp(4000L), 5))
        inputStream.addData((new Timestamp(now), 6), (new Timestamp(11000), 7))
      }
    }).start()

    query.awaitTermination(45000)

    val readValues = InMemoryKeyedStore.getValues(testKey)
    readValues should have size 2
    readValues should contain allOf("1970-01-01 01:00:10.0 -> 4", "1970-01-01 01:00:11.0 -> 1")
  }

  "the count on non-watermark column" should "be correctly computed in update mode" in {
    val testKey = "update-output-mode-without-watermark-aggregation"
    val inputStream = new MemoryStream[(Timestamp, Int)](1, sparkSession.sqlContext)
    val now = 5000L
    val aggregatedStream = inputStream.toDS().toDF("created", "number")
      .withWatermark("created", "1 second")
      .groupBy("number")
      .count()

    val query = aggregatedStream.writeStream.outputMode("update")
      .foreach(new InMemoryStoreWriter[Row](testKey,
        (processedRow) => s"${processedRow.getAs[Long]("number")} -> ${processedRow.getAs[Long]("count")}")).start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        inputStream.addData((new Timestamp(now+5000), 1), (new Timestamp(now+5000), 2), (new Timestamp(now+5000), 3),
          (new Timestamp(now+5000), 3))
        while (!query.isActive) {}
        Thread.sleep(10000)
        inputStream.addData((new Timestamp(4000L), 6))
        inputStream.addData((new Timestamp(now), 6), (new Timestamp(11000), 7))
      }
    }).start()

    query.awaitTermination(45000)

    val readValues = InMemoryKeyedStore.getValues(testKey)
    readValues should have size 5
    readValues should contain allOf("1 -> 1", "3 -> 2", "2 -> 1", "6 -> 2", "7 -> 1")
  }


  "the count on watermark column" should "be correctly computed in complete mode" in {
    val testKey = "update-output-mode-with-watermark-aggregation"
    val inputStream = new MemoryStream[(Timestamp, Int)](1, sparkSession.sqlContext)
    val now = 5000L
    val aggregatedStream = inputStream.toDS().toDF("created", "number")
      .withWatermark("created", "1 second")
      .groupBy("created")
      .count()

    val query = aggregatedStream.writeStream.outputMode("complete")
      .foreach(new InMemoryStoreWriter[Row](testKey,
        (processedRow) => s"${processedRow.getAs[Long]("created")} -> ${processedRow.getAs[Long]("count")}")).start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        inputStream.addData((new Timestamp(now+5000), 1), (new Timestamp(now+5000), 2), (new Timestamp(now+5000), 3),
          (new Timestamp(now+5000), 4))
        while (!query.isActive) {}
        Thread.sleep(10000)
        inputStream.addData((new Timestamp(4000L), 5))
        inputStream.addData((new Timestamp(now), 6), (new Timestamp(11000), 7))
      }
    }).start()

    query.awaitTermination(45000)

    val readValues = InMemoryKeyedStore.getValues(testKey)
    println(s"${readValues}")
    readValues should have size 5
    readValues.sorted should equal (Seq("1970-01-01 01:00:04.0 -> 1", "1970-01-01 01:00:05.0 -> 1",
      "1970-01-01 01:00:10.0 -> 4", "1970-01-01 01:00:10.0 -> 4", "1970-01-01 01:00:11.0 -> 1"))
  }

  "the count on non-watermark column" should "be correctly computed in complete mode" in {
    val testKey = "update-output-mode-with-watermark-aggregation"
    val inputStream = new MemoryStream[(Timestamp, Int)](1, sparkSession.sqlContext)
    val now = 5000L
    val aggregatedStream = inputStream.toDS().toDF("created", "number")
      .withWatermark("created", "1 second")
      .groupBy("number")
      .count()

    val query = aggregatedStream.writeStream.outputMode("complete")
      .foreach(new InMemoryStoreWriter[Row](testKey,
        (processedRow) => s"${processedRow.getAs[Long]("number")} -> ${processedRow.getAs[Long]("count")}")).start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        inputStream.addData((new Timestamp(now+5000), 1), (new Timestamp(now+5000), 2), (new Timestamp(now+5000), 3),
          (new Timestamp(now+5000), 4))
        while (!query.isActive) {}
        Thread.sleep(10000)
        inputStream.addData((new Timestamp(4000L), 5))
        inputStream.addData((new Timestamp(now), 6), (new Timestamp(11000), 7))
      }
    }).start()

    query.awaitTermination(45000)

    val readValues = InMemoryKeyedStore.getValues(testKey)
    println(s"${readValues}")
    readValues should have size 11
    readValues.sorted should equal (Seq("1 -> 1", "1 -> 1", "2 -> 1", "2 -> 1",
      "3 -> 1", "3 -> 1", "4 -> 1", "4 -> 1", "5 -> 1", "6 -> 1", "7 -> 1"))
  }
}
