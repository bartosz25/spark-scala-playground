package com.waitingforcode.structuredstreaming.join

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

class StateKeyWatermarkTest extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfter  {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming inner join state key watermark test")
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  before {
    TestedValuesContainer.values.clear()
  }

  "state key watermark" should "be built from watermark used in join" in {
    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)

    val mainEventsDataset = mainEventsStream.toDS().select($"mainKey", $"mainEventTime", $"mainEventTimeWatermark")
      .withWatermark("mainEventTimeWatermark", "2 seconds")
    val joinedEventsDataset = joinedEventsStream.toDS().select($"joinedKey", $"joinedEventTime", $"joinedEventTimeWatermark")
      .withWatermark("joinedEventTimeWatermark", "2 seconds")

    val stream = mainEventsDataset.join(joinedEventsDataset, mainEventsDataset("mainKey") === joinedEventsDataset("joinedKey") &&
      mainEventsDataset("mainEventTimeWatermark") === joinedEventsDataset("joinedEventTimeWatermark"))

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
          //println(s"Sending key=${key} (${new Timestamp(startingTime)}) for watermark ${query.lastProgress.eventTime.get("watermark")}")
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
  }

  "state key watermark" should "be built from watermark used in one side of join" in {
    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)

    val mainEventsDataset = mainEventsStream.toDS().select($"mainKey", $"mainEventTime", $"mainEventTimeWatermark")
      .withWatermark("mainEventTimeWatermark", "2 seconds")
    val joinedEventsDataset = joinedEventsStream.toDS().select($"joinedKey", $"joinedEventTime", $"joinedEventTimeWatermark")

    val stream = mainEventsDataset.join(joinedEventsDataset, mainEventsDataset("mainKey") === joinedEventsDataset("joinedKey") &&
      mainEventsDataset("mainEventTimeWatermark") === joinedEventsDataset("joinedEventTimeWatermark"))

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
  }

  "state key watermark" should "be built from watermark used in join window" in {
    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)

    val mainEventsDataset = mainEventsStream.toDS().select($"mainKey", $"mainEventTime", $"mainEventTimeWatermark",
      window($"mainEventTimeWatermark", "5 seconds").as("watermarkWindow")).withWatermark("watermarkWindow", "5 seconds")
    val joinedEventsDataset = joinedEventsStream.toDS().select($"joinedKey", $"joinedEventTime", $"joinedEventTimeWatermark",
      window($"joinedEventTimeWatermark", "5 seconds").as("watermarkWindow")).withWatermark("watermarkWindow", "5 seconds")

    val stream = mainEventsDataset.join(joinedEventsDataset, mainEventsDataset("mainKey") === joinedEventsDataset("joinedKey") &&
      mainEventsDataset("watermarkWindow") === joinedEventsDataset("watermarkWindow"))

    val query = stream.writeStream.trigger(Trigger.ProcessingTime(5000L)).foreach(RowProcessor).start()

    while (!query.isActive) {}
    new Thread(new Runnable() {
      override def run(): Unit = {
        val stateManagementHelper = new StateManagementHelper(mainEventsStream, joinedEventsStream)
        var key = 0
        val processingTimeFrom1970 = 0
        stateManagementHelper.waitForWatermarkToChange(query, processingTimeFrom1970)
        println("progress changed, got watermark" + query.lastProgress.eventTime.get("watermark"))
        key = 2
        var startingTime = stateManagementHelper.getCurrentWatermarkMillisInUtc(query)
        while (query.isActive) {
          val joinedKeyTime = if (key % 2 == 0) {
            startingTime
          } else {
            // for odd keys we define the time for previous window
            startingTime - 6000L
          }
          stateManagementHelper.sendPairedKeysWithSleep(s"key${key}", startingTime, Some(joinedKeyTime))
          startingTime += 1000L
          key += 1
        }
      }
    }).start()
    query.awaitTermination(90000)

    val allKeys = TestedValuesContainer.values.groupBy(testedValues => testedValues.key).keys
    val oddNumberKeys = allKeys.map(key => key.substring(3).toInt).filter(key => key > 1 && key % 2 != 0)
    oddNumberKeys shouldBe empty
  }

}
