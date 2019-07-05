package com.waitingforcode.structuredstreaming.join

import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicInteger

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

case class MainEvent(mainKey: String, mainEventTime: Long, mainEventTimeWatermark: Timestamp) {
}

case class JoinedEvent(joinedKey: String, joinedEventTime: Long, joinedEventTimeWatermark: Timestamp)

object Events {
  def joined(key: String, eventTime: Long = System.currentTimeMillis()): JoinedEvent = {
    JoinedEvent(key, eventTime, new Timestamp(eventTime))
  }
}