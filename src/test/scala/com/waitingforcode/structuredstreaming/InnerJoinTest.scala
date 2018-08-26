package com.waitingforcode.structuredstreaming

import java.sql.Timestamp

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class InnerJoinTest extends FlatSpec with Matchers with BeforeAndAfterAll  {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming inner join test")
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._


  behavior of "inner stream-to-stream join"

  it should "output the result as soon as it arrives" in {
    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)
    new Thread(new Runnable() {
      override def run(): Unit = {
        var key = 0
        while (true) {
          // Here main event are always sent before the joined
          // But we also send, an event for key - 5 in order to see if the main event is still kept in state store
          joinedEventsStream.addData(Events.joined(s"key${key-5}"))
          val mainEventTime = System.currentTimeMillis()
          mainEventsStream.addData(MainEvent(s"key${key}", mainEventTime, new Timestamp(mainEventTime)))
          Thread.sleep(500L)
          joinedEventsStream.addData(Events.joined(s"key${key}"))
          Thread.sleep(1000L)
          joinedEventsStream.addData(Events.joined(s"key${key}"))
          key += 1
        }
      }
    }).start()
    val mainEventTime = System.currentTimeMillis()
    mainEventsStream.addData(MainEvent("key_null", mainEventTime, new Timestamp(mainEventTime)))
    joinedEventsStream.addData(Events.joined("key_null"))

    val stream = mainEventsStream.toDS().join(joinedEventsStream.toDS(), $"mainKey" === $"joinedKey")

    val query = stream.writeStream.foreach(new ForeachWriter[Row] {
      override def process(value: Row): Unit = {
        println(s"processing ${value.mkString(",")} at ${System.currentTimeMillis()}")
      }

      override def close(errorOrNull: Throwable): Unit = {}

      override def open(partitionId: Long, version: Long): Boolean = true
    }).start()

    query.awaitTermination(40000)

    // TODO : key8 is output before and after --> does it mean that the state is held indifinetely by default ?
  }

  it should "ignore rows behind the watermark" in {
    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)
    import org.apache.spark.sql.functions._
    // Watermark is of 2 seconds so the rows coming after (i.e. key-5) should be ignored
    val stream = mainEventsStream.toDS().withWatermark("mainEventTimeWatermark", "2 seconds")
      .join(joinedEventsStream.toDS().withWatermark("joinedEventTimeWatermark", "2 seconds"),
        expr("mainKey = joinedKey AND joinedEventTimeWatermark > mainEventTimeWatermark AND joinedEventTimeWatermark <= mainEventTimeWatermark + interval 2 seconds"))
//        ($"mainKey" === $"joinedKey").and($"joinedEventTime" > $"mainEventTime")
//          .and($"joinedEventTime" <= $"mainEventTime".plus("interval 2 seconds")))

    val query = stream.writeStream/*.trigger(Trigger.ProcessingTime("3 seconds"))*/.foreach(new ForeachWriter[Row] {
      override def process(value: Row): Unit = {
        println(s"processing ${value.mkString(",")} at ${new Timestamp(System.currentTimeMillis())}")
      }

      override def close(errorOrNull: Throwable): Unit = {}

      override def open(partitionId: Long, version: Long): Boolean = true
    }).start()


    new Thread(new Runnable() {
      override def run(): Unit = {
        //Thread.sleep(10000)
        var key = 0
        while (true) {
          println(s"Sending key ${key} and ${key-5}")
          // Here main event are always sent before the joined
          // But we also send, an event for key - 5 in order to see if the main event is still kept in state store
          joinedEventsStream.addData(Events.joined(s"key${key-5}", System.currentTimeMillis()-33500L))
          val mainEventTime = System.currentTimeMillis()
          mainEventsStream.addData(MainEvent(s"key${key}", mainEventTime, new Timestamp(mainEventTime)))
          Thread.sleep(1500L)
          joinedEventsStream.addData(Events.joined(s"key${key}"))
          key += 1
          //Thread.sleep(2000) // ==> more than the watermark
        }
      }
    }).start()

    query.awaitTermination(120000)


  }


}
