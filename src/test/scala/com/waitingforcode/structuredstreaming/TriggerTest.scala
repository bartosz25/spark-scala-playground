package com.waitingforcode.structuredstreaming

import java.time.Instant

import com.waitingforcode.util.{InMemoryLogAppender, LogMessage, NoopForeachWriter}
import org.apache.log4j.spi.LoggingEvent
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class TriggerTest extends FlatSpec with Matchers {

  val sparkSession: SparkSession = SparkSession.builder().appName("Spark Structured Streaming output modes")
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  "once trigger" should "execute the query only once" in {
    val inputStream = new MemoryStream[Int](1, sparkSession.sqlContext)
    inputStream.addData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val stream = inputStream.toDS().toDF("number")

    val query = stream.writeStream.outputMode("update").trigger(Trigger.Once())
      .foreach(new NoopForeachWriter[Row]())
      .start()

    // Put here a very big timeout
    // The test will end much more before this time and it proves that
    // the query is executed only once, as the trigger defines it
    val queryStartTime = System.currentTimeMillis()
    query.awaitTermination(Long.MaxValue)
    val queryEndTime = System.currentTimeMillis()

    val queryExecutionTime = queryEndTime - queryStartTime
    queryExecutionTime should be < Long.MaxValue
  }

  "the information about trigger" should "be available in the last progress object" in {
    val inputStream = new MemoryStream[Int](1, sparkSession.sqlContext)
    new Thread(new Runnable() {
      override def run(): Unit = {
        while (true) {
          inputStream.addData(1 to 30)
          Thread.sleep(1000)
        }
      }
    }).start()
    val stream = inputStream.toDS().toDF("number")

    val query = stream.writeStream.outputMode("update").trigger(Trigger.ProcessingTime("5 seconds"))
      .foreach(new NoopForeachWriter[Row]())
      .start()

    query.awaitTermination(10000)

    val lastProgress = query.lastProgress
    val progressJson = lastProgress.json
    // The result should be similar to:
    // {"id":"41fb220b-5fc7-456b-b104-f49d374f25d8","runId":"4aaf947f-1747-43c3-a422-0f6209c26709","name":null,
    // "timestamp":"2018-02-11T11:21:25.000Z","numInputRows":480,"inputRowsPerSecond":122.88786482334869,
    // "processedRowsPerSecond":1467.8899082568807,"durationMs":{"addBatch":127,"getBatch":135,"getOffset":0,
    // "queryPlanning":36,"triggerExecution":326,"walCommit":18},
    // "stateOperators":[],
    // "sources":[{"description":"MemoryStream[value#1]","startOffset":2,"endOffset":6,"numInputRows":480,
    // "inputRowsPerSecond":122.88786482334869,"processedRowsPerSecond":1467.8899082568807}],
    // "sink":{"description":"org.apache.spark.sql.execution.streaming.ForeachSink@5192026d"}}
    progressJson should include ("\"triggerExecution\":")
  }

  "processing time-based trigger" should "be executed the time defined in the query" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("Starting Trigger Calculation"),
      (loggingEvent: LoggingEvent) => LogMessage(s"${loggingEvent.timeStamp}", ""))
    val inputStream = new MemoryStream[Int](1, sparkSession.sqlContext)
    new Thread(new Runnable() {
      override def run(): Unit = {
        while (true) {
          inputStream.addData(1)
          Thread.sleep(5000)
        }
      }
    }).start()
    val stream = inputStream.toDS().toDF("number")

    val query = stream.writeStream.outputMode("update").trigger(Trigger.ProcessingTime("1 seconds"))
      .foreach(new NoopForeachWriter[Row]())
      .start()

    query.awaitTermination(15000)

    val triggerExecutionTimes = logAppender.getMessagesText().map(_.toLong)
    val triggersIntervals = mutable.HashMap[Long, Int]()
    for (index <- 0 until triggerExecutionTimes.size - 1) {
      val currentTimestamp = triggerExecutionTimes(index)
      val nextTimestamp = triggerExecutionTimes(index+1)

      val triggerDiff = nextTimestamp - currentTimestamp

      val currentCount = triggersIntervals.getOrElse(triggerDiff, 0)
      val newCount = currentCount + 1
      triggersIntervals.put(triggerDiff, newCount)
    }
    // Output example:
    // triggersIntervals = Map(1001 -> 1, 1434 -> 1, 1000 -> 10, 895 -> 1, 999 -> 1)
    // As you can see, sometimes the difference is +/- 1 sec because of the time took for process the data
    // It proves that the trigger was executed every ~1 second. But as you can note in the
    // next test, the trigger launch doesn't mean data processing
    triggersIntervals should contain key 1000L
  }

  "trigger lower than data arrival time" should "not process rows every trigger interval" in {
    val inputStream = new MemoryStream[Int](1, sparkSession.sqlContext)
    val stream = inputStream.toDS().toDF("number")

    val query = stream.writeStream.outputMode("update").trigger(Trigger.ProcessingTime("1 seconds"))
      .foreach(new ForeachWriter[Row]() {
        override def open(partitionId: Long, version: Long): Boolean = true

        override def process(value: Row): Unit = {
          val currentTime = System.currentTimeMillis()
          Container.processingTimes.append(currentTime)
        }

        override def close(errorOrNull: Throwable): Unit = {}
      })
      .start()
    new Thread(new Runnable() {
      override def run(): Unit = {
        while (!query.isActive) {}
        while (true) {
          inputStream.addData(1)
          Thread.sleep(5000)
        }
      }
    }).start()

    query.awaitTermination(30000)

    val processingTimes = new mutable.ListBuffer[Long]()
    for (index <- 0 until Container.processingTimes.size - 1) {
      val currentTimestamp = Container.processingTimes(index)
      val nextTimestamp = Instant.ofEpochMilli(Container.processingTimes(index+1))

      val processingTimeDiffInSec = nextTimestamp.minusMillis(currentTimestamp).getEpochSecond
      processingTimes.append(processingTimeDiffInSec)
    }
    processingTimes should not be empty
    // Even though the trigger was defined to 1 second, we can see that the data processing is not launched
    // when there is no new data to process. So logically we should't find any difference
    // of 1 second between trigger subsequent executions for data processing
    processingTimes should not contain(1L)
  }
}

object Container {
  val processingTimes = new ListBuffer[Long]()
}