package com.waitingforcode.structuredstreaming.join

import java.sql.Timestamp

import com.waitingforcode.structuredstreaming.{Events, JoinedEvent, MainEvent}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQuery
import org.joda.time.{DateTimeZone, LocalDateTime}
import org.joda.time.format.DateTimeFormat

class StateManagementHelper(mainEventsStream: MemoryStream[MainEvent], joinedEventsStream: MemoryStream[JoinedEvent]) {

  def waitForWatermarkToChange(query: StreamingQuery, initialProcessingTime: Long): Unit = {
    while (query.lastProgress == null ||
      query.lastProgress.eventTime.toString == "{}" ||
      query.lastProgress.eventTime.get("watermark") == "1970-01-01T00:00:00.000Z") {
      Thread.sleep(2000L)
      mainEventsStream.addData(MainEvent(s"key1", initialProcessingTime, new Timestamp(initialProcessingTime)))
      joinedEventsStream.addData(Events.joined(s"key1", eventTime = initialProcessingTime))
      TestedValuesContainer.key1Counter.incrementAndGet()
      println(s"processingTimeFrom1970=${new LocalDateTime(initialProcessingTime).toDateTime(DateTimeZone.UTC)}")
    }
  }

  def getCurrentWatermarkMillisInUtc(query: StreamingQuery): Long = {
    val watermarkPattern = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    val localTime = LocalDateTime.parse(query.lastProgress.eventTime.get("watermark"), watermarkPattern)
    println(s">> got localTime=${localTime} and ms=${localTime.toDateTime(DateTimeZone.UTC).getMillis}")
    localTime.toDateTime(DateTimeZone.UTC).getMillis
  }

  def sendPairedKeysWithSleep(key: String, eventTimeMillis: Long, eventTimeJoined: Option[Long] = None): Unit = {
    val eventTimeForJoinedSide = eventTimeJoined.getOrElse(eventTimeMillis)
    mainEventsStream.addData(MainEvent(key, eventTimeMillis, new Timestamp(eventTimeMillis)))
    joinedEventsStream.addData(Events.joined(key, eventTime = eventTimeForJoinedSide))
    Thread.sleep(500L)
  }

  private val mainEvents = new scala.collection.mutable.ListBuffer[MainEvent]()
  private val joinedEvents = new scala.collection.mutable.ListBuffer[JoinedEvent]()
  private val BufferLimit = 10
  def collectAndSend(key: String, eventTimeMillis: Long, eventTimeJoined: Option[Long] = None, withJoinedEvent: Boolean): Boolean = {
    if (withJoinedEvent) {
      joinedEvents.append(Events.joined(key, eventTime = eventTimeJoined.get))
    }
    mainEvents.append(MainEvent(key, eventTimeMillis, new Timestamp(eventTimeMillis)))

    if (mainEvents.size == BufferLimit || joinedEvents.size == BufferLimit) {
      mainEventsStream.addData(mainEvents)
      joinedEventsStream.addData(joinedEvents)
      mainEvents.clear()
      joinedEvents.clear()
      true
    } else {
      false
    }
  }

}
