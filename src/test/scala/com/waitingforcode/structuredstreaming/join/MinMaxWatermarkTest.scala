package com.waitingforcode.structuredstreaming.join

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.scalatest.{FlatSpec, Matchers}

class MinMaxWatermarkTest extends FlatSpec with Matchers  {

  "min watermark" should "be used when the min policy is configured" in {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Spark Structured Streaming min").config("spark.sql.streaming.multipleWatermarkPolicy", "min")
      .master("local[3]").getOrCreate() // 3 executors are required to good execution of this test, at least 4 cores should be available
    import sparkSession.implicits._

    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)

    val mainEventsDataset = mainEventsStream.toDS().select($"mainKey", $"mainEventTime", $"mainEventTimeWatermark")
      .withWatermark("mainEventTimeWatermark", "4 seconds")
    val joinedEventsDataset = joinedEventsStream.toDS().select($"joinedKey", $"joinedEventTime", $"joinedEventTimeWatermark")
      .withWatermark("joinedEventTimeWatermark", "8 seconds")

    val stream = mainEventsDataset.join(joinedEventsDataset, mainEventsDataset("mainKey") === joinedEventsDataset("joinedKey") &&
      mainEventsDataset("mainEventTimeWatermark") >= joinedEventsDataset("joinedEventTimeWatermark"),
      "leftOuter")

    val query = stream.writeStream.trigger(Trigger.ProcessingTime(3000L)).foreach(RowProcessor).start()

    while (!query.isActive) {}
    launchDataInjection(mainEventsStream, joinedEventsStream, query)
    query.awaitTermination(150000)

    // Do not assert on the watermark directly - it can change depending on the execution environment
    // We simply checks the first known watermark value after the change
    FirstWatermark.FirstKnownValue shouldEqual "1970-01-01T00:00:02.000Z"
  }

  "max watermark" should "be used when the max policy is configured" in {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Spark Structured Streaming min").config("spark.sql.streaming.multipleWatermarkPolicy", "max")
      .master("local[3]").getOrCreate() // 3 executors are required to good execution of this test, at least 4 cores should be available
    import sparkSession.implicits._

    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)

    // Max = 06, hence it takes 4 seconds
    // Min = 02, hence it takes 8 seconds
    val mainEventsDataset = mainEventsStream.toDS().select($"mainKey", $"mainEventTime", $"mainEventTimeWatermark")
      .withWatermark("mainEventTimeWatermark", "4 seconds")
    val joinedEventsDataset = joinedEventsStream.toDS().select($"joinedKey", $"joinedEventTime", $"joinedEventTimeWatermark")
      .withWatermark("joinedEventTimeWatermark", "8 seconds")

    val stream = mainEventsDataset.join(joinedEventsDataset, mainEventsDataset("mainKey") === joinedEventsDataset("joinedKey") &&
      mainEventsDataset("mainEventTimeWatermark") >= joinedEventsDataset("joinedEventTimeWatermark"),
      "leftOuter")

    val query = stream.writeStream.trigger(Trigger.ProcessingTime(3000L)).foreach(RowProcessor).start()

    while (!query.isActive) {}
    launchDataInjection(mainEventsStream, joinedEventsStream, query)
    query.awaitTermination(150000)

    FirstWatermark.FirstKnownValue shouldEqual "1970-01-01T00:00:06.000Z"
  }

  def launchDataInjection(mainEventsStream: MemoryStream[MainEvent],
                          joinedEventsStream: MemoryStream[JoinedEvent], query: StreamingQuery): Unit = {
    new Thread(new Runnable() {
      override def run(): Unit = {
        val stateManagementHelper = new StateManagementHelper(mainEventsStream, joinedEventsStream)
        var key = 0
        val processingTimeFrom1970 = 10000L // 10 sec
        stateManagementHelper.waitForWatermarkToChange(query, processingTimeFrom1970)
        FirstWatermark.FirstKnownValue = query.lastProgress.eventTime.get("watermark")
        key = 2
        // We send keys: 2, 3, 4, 5, 6  in late to see watermark applied
        var startingTime = stateManagementHelper.getCurrentWatermarkMillisInUtc(query) - 5000L
        while (query.isActive) {
          if (key % 2 == 0) {
            stateManagementHelper.sendPairedKeysWithSleep(s"key${key}", startingTime)
          } else {
            mainEventsStream.addData(MainEvent(s"key${key}", startingTime, new Timestamp(startingTime)))
          }
          startingTime += 1000L
          key += 1
        }
      }
    }).start()
  }

}

object FirstWatermark {
  var FirstKnownValue = ""
}