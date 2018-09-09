package com.waitingforcode.structuredstreaming.join

import java.sql.Timestamp

import com.waitingforcode.structuredstreaming.{Events, JoinedEvent, MainEvent}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.window
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}


class InnerJoinTest extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfter  {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming inner join test")
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  before {
    TestedValuesContainer.values.clear()
  }

  behavior of "inner stream-to-stream join"

  it should "output the result as soon as it arrives without watermark" in {
    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)

    val stream = mainEventsStream.toDS().join(joinedEventsStream.toDS(), $"mainKey" === $"joinedKey")

    val query = stream.writeStream.foreach(RowProcessor).start()

    while (!query.isActive) {}
    new Thread(new Runnable() {
      override def run(): Unit = {
        var key = 0
        while (true) {
          // Here main event are always sent before the joined
          // But we also send, an event for key - 10 in order to see if the main event is still kept in state store
          joinedEventsStream.addData(Events.joined(s"key${key-10}"))
          val mainEventTime = System.currentTimeMillis()
          mainEventsStream.addData(MainEvent(s"key${key}", mainEventTime, new Timestamp(mainEventTime)))
          Thread.sleep(1000L)
          joinedEventsStream.addData(Events.joined(s"key${key}"))
          key += 1
        }
      }
    }).start()
    query.awaitTermination(60000)

    // As you can see in this test, when neither watermark nor range condition is defined, the state isn't cleared
    // It's why we can see data came 9/10 seconds after the first joined event of the same key
    val groupedByKeys = TestedValuesContainer.values.groupBy(testedValues => testedValues.key)
    val keysWith2Entries = groupedByKeys.filter(keyWithEntries => keyWithEntries._2.size == 2)
    keysWith2Entries.foreach(keyWithEntries => {
      val entries = keyWithEntries._2
      val metric1 = entries(0)
      val metric2 = entries(1)
      val diffBetweenEvents = metric2.joinedEventMillis.get - metric1.joinedEventMillis.get
      val timeDiffSecs = diffBetweenEvents/1000
      (timeDiffSecs >= 9 && timeDiffSecs <= 10) shouldBe true
    })
  }

  it should "join rows per windows" in {
    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)

    val mainEventsDataset = mainEventsStream.toDS().select($"mainKey", window($"mainEventTimeWatermark", "5 seconds"),
      $"mainEventTime", $"mainEventTimeWatermark")
    val joinedEventsDataset = joinedEventsStream.toDS().select($"joinedKey", window($"joinedEventTimeWatermark", "5 seconds"),
      $"joinedEventTime", $"joinedEventTimeWatermark")
    val stream = mainEventsDataset.join(joinedEventsDataset, mainEventsDataset("mainKey") === joinedEventsDataset("joinedKey") &&
      mainEventsDataset("window") === joinedEventsDataset("window"))

    val query = stream.writeStream.foreach(RowProcessor).start()

    while (!query.isActive) {}
    new Thread(new Runnable() {
      override def run(): Unit = {
        var key = 0
        var iterationTimeFrom1970 = 1000L // 1 sec
        while (query.isActive) {
          val (key1, key2) = (key + 1, key + 2)
          // join window is of 5 seconds so joining the value 6 seconds later (1 sec of sleep * 6)
          // should exclude given row from the join. Thus, at the end we should retrieve only rows with even keys
          joinedEventsStream.addData(Events.joined(s"key${key1-6}", eventTime = iterationTimeFrom1970))
          mainEventsStream.addData(MainEvent(s"key${key1}", iterationTimeFrom1970, new Timestamp(iterationTimeFrom1970)),
            MainEvent(s"key${key2}", iterationTimeFrom1970, new Timestamp(iterationTimeFrom1970)))
          Thread.sleep(1000L)
          joinedEventsStream.addData(Events.joined(s"key${key2}", eventTime = iterationTimeFrom1970))
          iterationTimeFrom1970 += iterationTimeFrom1970
          key += 2
        }
      }
    }).start()
    query.awaitTermination(60000)

    // Because rows with odd key are joined in late (outside the 5 seconds window), we should find
    // here only rows with even keys
    val processedEventsKeys = TestedValuesContainer.values.groupBy(testedValues => testedValues.key)
    processedEventsKeys.keys.foreach(key => {
      val keyNumber = key.substring(3).toInt
      keyNumber % 2 == 0 shouldBe true
    })
  }

  it should "filter and map before joining" in {
    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)

    val mainEventsWithMappedKey = mainEventsStream.toDS().filter(mainEvent => mainEvent.mainKey.length > 3)
      .map(mainEvent => mainEvent.copy(mainKey = s"${mainEvent.mainKey}_copy"))
    // For nullable side we deliberately omit the filtering - it shows that the query
    // works even without some subtle differences
    val joinedEventsWithMappedKey = joinedEventsStream.toDS()
      .map(joinedEvent => joinedEvent.copy(joinedKey = s"${joinedEvent.joinedKey}_copy"))

    val stream = mainEventsWithMappedKey.join(joinedEventsWithMappedKey, $"mainKey" === $"joinedKey")

    val query = stream.writeStream.foreach(RowProcessor).start()

    while (!query.isActive) {}
    new Thread(new Runnable() {
      override def run(): Unit = {
        var key = 0
        while (query.isActive) {
          val eventTime = System.currentTimeMillis()
          mainEventsStream.addData(MainEvent(s"key${key}", eventTime, new Timestamp(eventTime)))
          joinedEventsStream.addData(Events.joined(s"key${key}", eventTime = eventTime))
          Thread.sleep(1000L)
          key += 1
        }
      }
    }).start()
    query.awaitTermination(60000)

    val groupedByKeys = TestedValuesContainer.values.groupBy(testedValues => testedValues.key)
    groupedByKeys.keys.foreach(key => {
      key should endWith("_copy")
    })
  }

  it should "fail when the aggregations are made before the join" in {
    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)

    val exception = intercept[AnalysisException] {
      val mainEventsWithMappedKey = mainEventsStream.toDS()
      val joinedEventsWithMappedKey = joinedEventsStream.toDS().groupBy($"joinedKey").count()

      val stream = mainEventsWithMappedKey.join(joinedEventsWithMappedKey, $"mainKey" === $"joinedKey")

      stream.writeStream.foreach(RowProcessor).start()
    }
    exception.getMessage() should include("Append output mode not supported when there are streaming aggregations " +
      "on streaming DataFrames/DataSets without watermark")
  }

}