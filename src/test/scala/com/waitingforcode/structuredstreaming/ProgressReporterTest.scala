package com.waitingforcode.structuredstreaming

import com.waitingforcode.util.NoopForeachWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQueryProgress, Trigger}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ProgressReporterTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder().appName("Spark Structured Streaming progress reporter")
    .master("local[2]").getOrCreate()
  import sparkSession.sqlContext.implicits._

  "sample count aggregation per id" should "have corresponding metricss" in {
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val aggregatedStream = inputStream.toDS().toDF("id", "name")
      .groupBy("id")
      .agg(count("*"))

    val query = aggregatedStream.writeStream.trigger(Trigger.ProcessingTime(1000)).outputMode("complete")
      .foreach(new NoopForeachWriter())
      .start()

    val progress = new scala.collection.mutable.ListBuffer[StreamingQueryProgress]()
    new Thread(new Runnable() {
      override def run(): Unit = {
        var currentBatchId = -1L
        while (query.isActive) {
          inputStream.addData((1, "A"), (2, "B"))
          Thread.sleep(1000)
          val lastProgress = query.lastProgress
          if (currentBatchId != lastProgress.batchId) {
            progress.append(lastProgress)
            currentBatchId = lastProgress.batchId
          }
        }
      }
    }).start()

    query.awaitTermination(45000)
    val firstProgress = progress(0)
    firstProgress.batchId shouldEqual(0L)
    firstProgress.stateOperators should have size 0
    firstProgress.numInputRows shouldEqual(0L)
    val secondProgress = progress(1)
    secondProgress.batchId shouldEqual(1L)
    // Below some metrics contained in the progress reported
    // It's impossible to provide exact numbers it's why the assertions are approximate
    secondProgress.durationMs.get("addBatch").toLong should be > 6000L
    secondProgress.durationMs.get("getBatch").toLong should be < 1000L
    secondProgress.durationMs.get("queryPlanning").toLong should be < 1000L
    secondProgress.durationMs.get("triggerExecution").toLong should be < 31000L
    secondProgress.durationMs.get("walCommit").toLong should be < 500L
    secondProgress.stateOperators(0).numRowsTotal should be < 10L
    secondProgress.stateOperators(0).numRowsTotal should be > 0L
    secondProgress.stateOperators(0).numRowsTotal shouldEqual(secondProgress.stateOperators(0).numRowsUpdated)
    secondProgress.stateOperators(0).memoryUsedBytes should be > 10000L
    secondProgress.sources should have size 1
    secondProgress.sources(0).startOffset shouldEqual("0")
    secondProgress.sources(0).numInputRows should be >= 20L
    secondProgress.sink.description.contains("ForeachWriterProvider") shouldBe true
  }

}
