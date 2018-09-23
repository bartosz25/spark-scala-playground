package com.waitingforcode.structuredstreaming.join

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

class StateValueWatermarkTest extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfter  {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming inner join state value watermark")
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  before {
    TestedValuesContainer.values.clear()
  }

  "state value watermark" should "be built from a watermark column and range condition" in {
    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)

    val mainEventsDataset = mainEventsStream.toDS().select($"mainKey", $"mainEventTime", $"mainEventTimeWatermark")
      .withWatermark("mainEventTimeWatermark", "2 seconds")
    val joinedEventsDataset = joinedEventsStream.toDS().select($"joinedKey", $"joinedEventTime", $"joinedEventTimeWatermark")
      .withWatermark("joinedEventTimeWatermark", "2 seconds")

    val stream = mainEventsDataset.join(joinedEventsDataset, mainEventsDataset("mainKey") === joinedEventsDataset("joinedKey") &&
      expr("joinedEventTimeWatermark > mainEventTimeWatermark + interval 2 seconds"))

    val query = stream.writeStream.trigger(Trigger.ProcessingTime(5000L)).foreach(RowProcessor).start()

    while (!query.isActive) {}
    new Thread(new Runnable() {
      override def run(): Unit = {
        val stateManagementHelper = new StateManagementHelper(mainEventsStream, joinedEventsStream)
        var key = 0
        val processingTimeFrom1970 = 10000L // 10 sec
        stateManagementHelper.waitForWatermarkToChange(query, processingTimeFrom1970)
        println("progre ss changed, got watermark" + query.lastProgress.eventTime.get("watermark"))
        key = 2
        var startingTime = stateManagementHelper.getCurrentWatermarkMillisInUtc(query)
        while (query.isActive) {
          val joinedSideEventTime = if (startingTime % 2000 == 0) {
            startingTime + 3000L
          } else {
            // the value computed like this is evidently after the watermark, so should be accepted in the stream
            // but since the range condition is stricter, the row will be ignored
            startingTime - 1000L
          }
          stateManagementHelper.sendPairedKeysWithSleep(s"key${key}", startingTime, Some(joinedSideEventTime))
          startingTime += 1000L
          key += 1
        }
      }
    }).start()
    query.awaitTermination(90000)

    val processedKeys = TestedValuesContainer.values.groupBy(testedValues => testedValues.key).keys
    val keyNumbers = processedKeys.map(key => key.substring(3).toInt)
    val oddKeyNumbers = keyNumbers.filter(keyNumber => keyNumber % 2 != 0)
    oddKeyNumbers shouldBe empty
  }

  "state value watermark" should "be built from a watermark window column and range condition" in {
    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)

    val mainEventsDataset = mainEventsStream.toDS().select($"mainKey", $"mainEventTime", $"mainEventTimeWatermark",
      window($"mainEventTimeWatermark", "3 seconds").as("mainWatermarkWindow")).withWatermark("mainWatermarkWindow", "3 seconds")
    val joinedEventsDataset = joinedEventsStream.toDS().select($"joinedKey", $"joinedEventTime", $"joinedEventTimeWatermark",
      window($"joinedEventTimeWatermark", "3 seconds").as("joinedWatermarkWindow")).withWatermark("joinedWatermarkWindow", "3 seconds")

    val stream = mainEventsDataset.join(joinedEventsDataset, mainEventsDataset("mainKey") === joinedEventsDataset("joinedKey") &&
      expr("joinedWatermarkWindow > mainWatermarkWindow"))

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
        var startingTime = stateManagementHelper.getCurrentWatermarkMillisInUtc(query)

        while (query.isActive) {
          val joinedSideEventTime = if (key % 2 == 0) {
            startingTime + 4000L
          } else {
            startingTime - 4000L
          }
          stateManagementHelper.sendPairedKeysWithSleep(s"key${key}", startingTime, Some(joinedSideEventTime))
          startingTime += 1000L
          key += 1
        }
      }
    }).start()
    query.awaitTermination(90000)

    val processedKeys = TestedValuesContainer.values.groupBy(testedValues => testedValues.key).keys
    processedKeys.nonEmpty shouldBe true
    val keyNumbers = processedKeys.map(key => key.substring(3).toInt)
    val oddKeyNumbers = keyNumbers.filter(keyNumber => keyNumber % 2 != 0)
    oddKeyNumbers shouldBe empty
  }

  "state value watermark" should "be built from different watermark columns and range condition" in {
    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)

    val mainEventsDataset = mainEventsStream.toDS().select($"mainKey", $"mainEventTime", $"mainEventTimeWatermark")
      .withWatermark("mainEventTimeWatermark", "2 seconds")
    // To see what happens, let's set the watermark of joined side to 10 times more than the main dataset
    val joinedEventsDataset = joinedEventsStream.toDS().select($"joinedKey", $"joinedEventTime", $"joinedEventTimeWatermark")
      .withWatermark("joinedEventTimeWatermark", "20 seconds")

    val stream = mainEventsDataset.join(joinedEventsDataset, mainEventsDataset("mainKey") === joinedEventsDataset("joinedKey") &&
      expr("joinedEventTimeWatermark > mainEventTimeWatermark + interval 2 seconds"))

    val query = stream.writeStream.trigger(Trigger.ProcessingTime(5000L)).foreach(RowProcessor).start()

    var firstWatermark: Option[String] = None
    while (!query.isActive) {}
    new Thread(new Runnable() {
      override def run(): Unit = {
        val stateManagementHelper = new StateManagementHelper(mainEventsStream, joinedEventsStream)
        var key = 0
        // 21 sec ==> watermark is MAX(event_time) - 20'' and lower value will never change it
        val processingTimeFrom1970 = 21000L
        stateManagementHelper.waitForWatermarkToChange(query, processingTimeFrom1970)
        println("progress changed, got watermark" + query.lastProgress.eventTime.get("watermark"))
        key = 2
        firstWatermark = Some(query.lastProgress.eventTime.get("watermark"))
        var startingTime = stateManagementHelper.getCurrentWatermarkMillisInUtc(query)
        while (query.isActive) {
          val joinedSideEventTime = if (startingTime % 2000 == 0) {
            startingTime + 3000L
          } else {
            startingTime - 1000L
          }
          stateManagementHelper.sendPairedKeysWithSleep(s"key${key}", startingTime, Some(joinedSideEventTime))
          startingTime += 1000L
          key += 1
        }
      }
    }).start()
    query.awaitTermination(90000)

    firstWatermark shouldBe defined
    firstWatermark.get shouldEqual "1970-01-01T00:00:01.000Z"
    val processedKeys = TestedValuesContainer.values.groupBy(testedValues => testedValues.key).keys
    // In this case we don't expect event numbers because odd numbers goes to the first sending condition and the others
    // to the second one
    val keyNumbers = processedKeys.map(key => key.substring(3).toInt)
    val evenKeyNumbers = keyNumbers.filter(keyNumber => keyNumber % 2 == 0)
    evenKeyNumbers shouldBe empty
  }

}
