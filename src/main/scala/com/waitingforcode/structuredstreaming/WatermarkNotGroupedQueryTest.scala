package com.waitingforcode.structuredstreaming

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import java.util.TimeZone

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger

/**
  * This code snippet illustrates when the watermark is used.
  * You can run it with true/false flag as parameter to enable/disable
  * the watermark. You will see that the watermark won't work for false flag
  * because there is no grouping on the watermark's column.
  */
object WatermarkNotGroupedQueryTest extends App {

  private val sparkSession: SparkSession = SparkSession.builder().appName("Watermark not grouped query")
    .config("spark.sql.session.timeZone", "UTC")
    .master("local[2]").getOrCreate()
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  import sparkSession.implicits._

  val inputStream = new MemoryStream[(Timestamp, String)](1, sparkSession.sqlContext)

  val eventTime_08_59 = utcTimestamp("2020-01-26T08:59:00")
  val eventTime_09_00 = utcTimestamp("2020-01-26T09:00:00")
  val eventTime_09_01 = utcTimestamp("2020-01-26T09:01:00")
  val eventTime_09_02 = utcTimestamp("2020-01-26T09:02:00")
  val eventTime_09_03 = utcTimestamp("2020-01-26T09:03:00")
  val eventTime_09_06 = utcTimestamp("2020-01-26T09:06:00")

  def utcTimestamp(dateTimeToConvert: String): Timestamp = {
    val utcMillis = LocalDateTime.parse(dateTimeToConvert).toInstant(ZoneOffset.UTC).toEpochMilli
    new Timestamp(utcMillis)
  }


  val batch1 = Seq(
    (eventTime_09_01, "a"), (eventTime_09_01, "a"),
    (eventTime_09_02, "b"), (eventTime_09_03, "c")
  )
  val batch2 = Seq(
    (eventTime_08_59, "a"), (eventTime_09_00, "c"),
    (eventTime_09_00, "a"), (eventTime_09_03, "c")
  )
  val batch3 = Seq(
    (eventTime_08_59, "b"), (eventTime_09_03, "a"),
    (eventTime_09_06, "b"), (eventTime_08_59, "a"),
    (eventTime_08_59, "a"),(eventTime_08_59, "a")
  )

  val isGrouped = args(0).toBoolean
  val streamQuery = if (isGrouped) {
    inputStream.toDS().toDF("created", "name")
      .withWatermark("created", "1 minute")
      .groupBy("created")
      .count()
      .writeStream.format("console").option("truncate", false)
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("update")
      .start()
  } else {
    inputStream.toDS().toDF("created", "name")
      .withWatermark("created", "1 minute")
      .agg(Map("name" -> "count"))
      .writeStream.format("console").option("truncate", false)
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("update")
      .start()
  }

  new Thread(new Runnable() {
    override def run(): Unit = {
      inputStream.addData(batch1)
      while (!streamQuery.isActive || streamQuery.lastProgress == null ||
        streamQuery.lastProgress.eventTime.get("watermark") == "1970-01-01T00:00:00.000Z") {
        // wait the query to activate
      }
      println(s"Last progress 0=${Option(streamQuery.lastProgress).map(r => r.prettyJson)}")
      Thread.sleep(21000)
      println(s"Last progress 1=${Option(streamQuery.lastProgress).map(r => r.prettyJson)}")
      inputStream.addData(batch2)
      streamQuery.explain(true)
      Thread.sleep(21000)
      println(s"Last progress 2=${Option(streamQuery.lastProgress).map(r => r.prettyJson)}")
      inputStream.addData(batch3)
    }
  }).start()


  streamQuery.awaitTermination()
}
