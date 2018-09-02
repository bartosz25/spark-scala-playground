package com.waitingforcode.structuredstreaming

import java.sql.Timestamp

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class OuterJoinTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming outer join test")
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  behavior of "structured streaming outer join"

  it should "xoooo" in {

    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)

    // Watermark is of 2 seconds so the rows coming after (i.e. key-5) should be ignored
    val stream = mainEventsStream.toDS().withWatermark("eventTimeWatermark", "2 seconds")
      .join(joinedEventsStream.toDS().withWatermark("eventTimeWatermark", "2 seconds"), $"mainKey" === $"joinedKey",
      "left_outer")

    val query = stream.writeStream/*.trigger(Trigger.ProcessingTime("3 seconds"))*/.foreach(new ForeachWriter[Row] {
      override def process(value: Row): Unit = {
        println(s"processing ${value.mkString(",")} at ${new Timestamp(System.currentTimeMillis())}")
      }

      override def close(errorOrNull: Throwable): Unit = {}

      override def open(partitionId: Long, version: Long): Boolean = true
    }).start()


    new Thread(new Runnable() {
      override def run(): Unit = {
        Thread.sleep(10000)
        var key = 0
        while (true) {
          println(s"Sending key ${key} and ${key-5}")
          // Here main event are always sent before the joined
          // But we also send, an event for key - 5 in order to see if the main event is still kept in state store
          joinedEventsStream.addData(Events.joined(s"key${key-5}", System.currentTimeMillis()-33500L))
          val mainEventTime = System.currentTimeMillis()
          mainEventsStream.addData(MainEvent(s"key${key}", mainEventTime, new Timestamp(mainEventTime)))
          Thread.sleep(500L)
          joinedEventsStream.addData(Events.joined(s"key${key}"))
          key += 1
          Thread.sleep(3000) // ==> more than the watermark
        }
      }
    }).start()

    query.awaitTermination(120000)
  }

}


case class MainEvent(mainKey: String, mainEventTime: Long, mainEventTimeWatermark: Timestamp) {
}

case class JoinedEvent(joinedKey: String, joinedEventTime: Long, joinedEventTimeWatermark: Timestamp)

object Events {
  def joined(key: String, eventTime: Long = System.currentTimeMillis()): JoinedEvent = {
    JoinedEvent(key, eventTime, new Timestamp(eventTime))
  }
}