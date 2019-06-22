package com.waitingforcode.sql

import com.waitingforcode.sql.IdempotentConsumerRowMapper.{dateTime, user}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.control.NonFatal
import scala.util.hashing.MurmurHash3

class IdempotentConsumerTest extends FlatSpec with Matchers {

  "an idempotent consumer" should "skip already read events" in {
    // I'm using here a batch processing for its simpler semantic for this example
    val sparkSession = SparkSession.builder().appName("Spark SQL idempotent consumer test").master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val events = Seq(
      IdempotentConsumerEvent(1, "2019-05-01T10:00"), IdempotentConsumerEvent(1, "2019-05-01T10:00"),
      IdempotentConsumerEvent(1, "2019-05-01T10:00"), IdempotentConsumerEvent(2, "2019-05-01T10:00"),
      IdempotentConsumerEvent(3, "2019-05-01T10:00"), IdempotentConsumerEvent(3, "2019-05-01T10:00"),
      IdempotentConsumerEvent(4, "2019-05-01T10:00"), IdempotentConsumerEvent(5, "2019-05-01T10:00")
    ).toDF

    events.map(row => IdentifiedIdempotentConsumerEven(IdempotentConsumerRowMapper.id(row), user(row), dateTime(row)))
      // Semantically I'm not using groupByKey because I don't want to create groups. Rather than that I want all events
      // with the same key on the same node to create groups of ids to query instead of querying them 1 by 1
      .repartitionByRange($"id")
      .mapPartitions(rows => {
        rows.grouped(100)
          .flatMap(groupedRows => {
            // Here 2 choices:
            // - using .hashCode() - not a lot of work but you're generating the id on all columns
            // - custom algorithm - more work but also more control
            // I chosen the latter one.
            val eventsIds = groupedRows.map(event => event.id)
            val alreadyProcessedIds = MessageStore.getRowsForIds(eventsIds).toSet

            val notProcessedEvents = groupedRows.filter(event => !alreadyProcessedIds.contains(event.id))
            notProcessedEvents.distinct
          })
      })
      .foreachPartition(notProcessedEvents => {
        val materializedEvents = notProcessedEvents.toSeq
        val eventsIds = materializedEvents.map(event => event.id)
        try {
          // do something with the events here
          // I'm using a big batch but maybe you can opt for micro-batches built
          // with Scala's .grouped(...)
          MessageStore.persist(eventsIds)
        } catch {
          case NonFatal(e) => {
            MessageStore.remove(eventsIds)
            throw e
          }
        }
     })

    MessageStore.newPersistedIds should have size 4
    MessageStore.newPersistedIds should contain allOf (-1660704958, -220168171, -209817447, -96024353)

  }

}

case class IdempotentConsumerEvent(user: Int, dateTime: String)
case class IdentifiedIdempotentConsumerEven(id: Int, user: Int, dateTime: String)

object IdempotentConsumerRowMapper {
  def user(row: Row): Int = row.getAs[Int]("user")
  def dateTime(row: Row): String = row.getAs[String]("dateTime")
  def id(row: Row): Int = MurmurHash3.stringHash(s"${user(row)}_${dateTime(row)}")
}

object MessageStore {

  // For now only the event corresponding to user=1 is considered as processed
  private val ids = Seq[Int](-284117173)

  private var newIds = Seq[Int]()
  def newPersistedIds = newIds

  def getRowsForIds(idsToCheck: Iterable[Int]): Seq[Int] = {
    ids.intersect(idsToCheck.toSeq)
  }

  def persist(newIdsToPersist: Seq[Int]) = {
    newIds ++= newIdsToPersist
  }

  def remove(ids: Seq[Int]): Unit = {
    newIds = newIds.diff(ids)
  }

}
