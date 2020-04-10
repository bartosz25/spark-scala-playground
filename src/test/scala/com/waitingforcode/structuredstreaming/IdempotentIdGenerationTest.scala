package com.waitingforcode.structuredstreaming

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.hashing.MurmurHash3

class IdempotentIdGenerationTest extends FlatSpec with Matchers {

  val sparkSession: SparkSession = SparkSession.builder().appName("Spark Structured Streaming idempotent " +
    "id generation")
    .master("local[*]").getOrCreate()
  import sparkSession.sqlContext.implicits._

  "timestamp field" should "guarantee idempotence" in {
    val inputStream = new MemoryStream[(String, Timestamp)](1, sparkSession.sqlContext)
    inputStream.addData(
      ("""{"entityId": 1, "eventTime": "2020-03-01T10:05:00+00:00", "value": "D"}""", new Timestamp(6L)),
      ("""{"entityId": 1, "eventTime": "2020-03-01T10:03:45+00:00", "value": "C"}""", new Timestamp(5L)),
      ("""{"entityId": 1, "eventTime": "2020-03-01T10:03:00+00:00", "value": "B"}""", new Timestamp(3L)),
      ("""{"entityId": 1, "eventTime": "2020-03-01T10:00:00+00:00", "value": "A"}""", new Timestamp(1L)),
      // Let's suppose that we missed this entry in the previous execution
      // Here in the reprocessing we include it and it can break the idempotence effort
      // if we used eventTime as the sort field
      ("""{"entityId": 1, "eventTime": "2020-03-01T09:57:00+00:00", "value": "E"}""", new Timestamp(10L))
    )
    val inputSchema = ScalaReflection.schemaFor[IdempotentIdGenerationValue].dataType.asInstanceOf[StructType]
    val inputData = inputStream.toDS().toDF("value", "timestamp")

    val mappedEntities = inputData.select(
      functions.from_json($"value", inputSchema),
      $"timestamp"
    ).as[(IdempotentIdGenerationValue, Timestamp)]
    .groupByKey(valueWithTimestamp => valueWithTimestamp._1.entityId)
    .mapGroups((_, groupValues) => {
      val sortedEvents = groupValues.toSeq.sortBy(valueWithTimestamp => valueWithTimestamp._2.getTime)

      val idKey = s"${sortedEvents.head._2.getTime}-${sortedEvents.head._1.entityId}"
      val sessionId = MurmurHash3.stringHash(idKey)

      IdempotentIdGenerationSession(sessionId, startTime=sortedEvents.head._1.eventTime,
        endTime=sortedEvents.last._1.eventTime)
    })

    val writeQuery = mappedEntities.writeStream.trigger(Trigger.Once).foreachBatch((dataset, _) => {
      IdempotentIdGenerationContainer.session = dataset.collect().head
    })

    writeQuery.start().awaitTermination()

    IdempotentIdGenerationContainer.session shouldEqual IdempotentIdGenerationSession(
      -949026733, Timestamp.valueOf("2020-03-01 11:00:00.0"), Timestamp.valueOf("2020-03-01 10:57:00.0")
    )
  }

}

case class IdempotentIdGenerationValue(entityId: Long, eventTime: Timestamp, value: String)
case class IdempotentIdGenerationSession(sessionId: Int, startTime: Timestamp, endTime: Timestamp)
object IdempotentIdGenerationContainer {
  var session: IdempotentIdGenerationSession = _
}