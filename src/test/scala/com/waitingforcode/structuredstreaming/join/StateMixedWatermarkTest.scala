package com.waitingforcode.structuredstreaming.join

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

class StateMixedWatermarkTest extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfter  {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming inner join state value watermark")
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  before {
    TestedValuesContainer.values.clear()
  }

  "mixed watermark" should "use stricter state key watermark" in {
    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)

    val mainEventsDataset = mainEventsStream.toDS().select($"mainKey", $"mainEventTime", $"mainEventTimeWatermark")
      .withWatermark("mainEventTimeWatermark", "2 seconds")
    val joinedEventsDataset = joinedEventsStream.toDS().select($"joinedKey", $"joinedEventTime", $"joinedEventTimeWatermark")
      .withWatermark("joinedEventTimeWatermark", "2 seconds")

    val stream = mainEventsDataset.join(joinedEventsDataset, mainEventsDataset("mainKey") === joinedEventsDataset("joinedKey") &&
      mainEventsDataset("mainEventTimeWatermark") === joinedEventsDataset("joinedEventTimeWatermark") &&
      expr("joinedEventTimeWatermark >= mainEventTimeWatermark - interval 2 seconds"))

    val query = stream.writeStream.trigger(Trigger.ProcessingTime(5000L)).foreach(RowProcessor).start()

    while (!query.isActive) {}
    new Thread(new Runnable() {
      override def run(): Unit = {
        val stateManagementHelper = new StateManagementHelper(mainEventsStream, joinedEventsStream)
        var key = 0
        val processingTimeFrom1970 = 10000L // 10 sec
        stateManagementHelper.waitForWatermarkToChange(query, processingTimeFrom1970)
        println("progress changed, got watermark" + query.lastProgress.eventTime.get("watermark"))
        key = 2
        // We send keys: 2, 3, 4, 5, 7  in late to see watermark applied
        var startingTime = stateManagementHelper.getCurrentWatermarkMillisInUtc(query) - 5000L
        while (query.isActive) {
          stateManagementHelper.sendPairedKeysWithSleep(s"key${key}", startingTime)
          startingTime += 1000L
          key += 1
        }
      }
    }).start()
    query.awaitTermination(90000)

    val groupedByKeys = TestedValuesContainer.values.groupBy(testedValues => testedValues.key)
    groupedByKeys.keys should not contain allOf("key2", "key3", "key4", "key5", "key6", "key7")
    // Check some initial keys that should be sent after the first generated watermark
    groupedByKeys.keys should contain allOf("key8", "key9", "key10")
    println(s"got keys=${groupedByKeys.mkString("\n")}")
  }

}
