package com.waitingforcode.structuredstreaming.join

import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{ForeachWriter, Row}

import scala.collection.mutable

object RowProcessor extends ForeachWriter[Row] {
  override def process(row: Row): Unit = {
    val generationTime = if (row.schema.fieldNames.contains("generationTime")) {
      Some(row.getAs[Timestamp]("generationTime").getTime)
    } else {
      None
    }
    val metric = new JoinResult(row.getAs[String]("mainKey"), row.getAs[Long]("mainEventTime"),
      Option(row.getAs[Long]("joinedEventTime")), System.currentTimeMillis(), generationTime)
    TestedValuesContainer.values.append(metric)
  }

  override def close(errorOrNull: Throwable): Unit = {}

  override def open(partitionId: Long, version: Long): Boolean = true
}

case class JoinResult(key: String, mainEventMillis: Long, joinedEventMillis: Option[Long], processingTime: Long,
                      rowCreationTime: Option[Long] = None)

object TestedValuesContainer {
  val values = new mutable.ListBuffer[JoinResult]()

  val key1Counter = new AtomicInteger(0)
}

class OneKeyDataSender(query: StreamingQuery, mainEventsStream: MemoryStream[MainEvent],
                       joinedEventsStream: MemoryStream[JoinedEvent]) {

  val thread = new Thread(new Runnable() {
    override def run(): Unit = {
      while (!query.isActive) {}
      var wasSent = false
      while (query.isActive) {
        Thread.sleep(1000L)
        if (!wasSent) {
          wasSent = true
          val mainEventTime = System.currentTimeMillis()
          mainEventsStream.addData(MainEvent(s"key1", mainEventTime, new Timestamp(mainEventTime)))
        }
        // Logically key1 should expire after 5 seconds so joined key shouldn't be really joined because of the watermark
        // on the main event
        joinedEventsStream.addData(Events.joined(s"key1"))
        TestedValuesContainer.key1Counter.incrementAndGet()
      }
    }
  })

}

case class MainEvent(mainKey: String, mainEventTime: Long, mainEventTimeWatermark: Timestamp) {
}

case class JoinedEvent(joinedKey: String, joinedEventTime: Long, joinedEventTimeWatermark: Timestamp)

object Events {
  def joined(key: String, eventTime: Long = System.currentTimeMillis()): JoinedEvent = {
    JoinedEvent(key, eventTime, new Timestamp(eventTime))
  }
}