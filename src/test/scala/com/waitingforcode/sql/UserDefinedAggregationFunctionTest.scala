package com.waitingforcode.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.immutable.TreeMap

class UserDefinedAggregationFunctionTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder().appName("UDAF test").master("local")
    .getOrCreate()

  import sparkSession.implicits._
  private val Sessions = Seq(
    (1, 100, "categories.html"), (2, 100, "index.html"), (3, 100, "contact.html"),
    (1, 150, "categories/home.html"), (1, 200, "cart.html"), (3, 300, "reclaim_form.html")
  ).toDF("user", "time", "page")

  override def afterAll() {
    sparkSession.stop()
  }

  "sessions" should "be aggregated with UDAF" in {
    val sessionsAggregator = new SessionsAggregator

    val aggregatedSessions = Sessions.groupBy("user").agg(sessionsAggregator(Sessions.col("time"), Sessions.col("page")))
      .collect()

    val expectedResults = Map(
      1 -> Map(100 -> "categories.html", 150 -> "categories/home.html", 200 -> "cart.html"),
      2 -> Map(100 -> "index.html"),
      3 -> Map(100 -> "contact.html", 300 -> "reclaim_form.html")
    )
    aggregatedSessions.foreach(aggregationResult => {
      val aggregationKey = aggregationResult.getInt(0)
      val sessionMap = aggregationResult.getMap[Long, String](1)
      sessionMap should equal(expectedResults(aggregationKey))
    })
  }

  "sessions length" should "be computed with UDAF registered with UDF" in {
    sparkSession.udf.register("SessionLength_registerTest", new SessionDurationAggregator)

    val sessionsDurations = Sessions.groupBy("user").agg("time" -> "SessionLength_registerTest")
      .collect()

    val expectedResults = Map(1 -> 100, 2 -> 0, 3 -> 200)
    sessionsDurations.foreach(aggregationResult => {
      val aggregationKey = aggregationResult.getInt(0)
      val sessionDuration = aggregationResult.getLong(1)
      sessionDuration should equal(expectedResults(aggregationKey))
    })
  }

}

class SessionsAggregator extends UserDefinedAggregateFunction {

  private val AggregationMapType = MapType(LongType, StringType, valueContainsNull = false)

  override def inputSchema: StructType = StructType(Seq(
    StructField("time", LongType, false), StructField("page", StringType, false)
  ))

  override def bufferSchema: StructType = StructType(Seq(
    StructField("visited_pages", AggregationMapType, false)
  ))

  override def dataType: DataType = AggregationMapType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, new TreeMap[Long, String]())
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val currentTimes = buffer.getMap[Long, String](0)
    val newTimes = currentTimes + (input.getLong(0) -> input.getString(1))
    buffer.update(0, newTimes)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val currentTimes = buffer1.getMap[Long, String](0)
    val newTimes = currentTimes ++ buffer2.getMap[Long, String](0)
    buffer1.update(0, newTimes)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getMap(0)
  }
}

class SessionDurationAggregator extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(Seq(StructField("time", LongType, false)))

  override def bufferSchema: StructType = StructType(Seq(
    StructField("first_log_time", LongType, false), StructField("last_log_time", LongType, false)
  ))

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, Long.MaxValue)
    buffer.update(1, Long.MinValue)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val sessionStartTime = buffer.getLong(0)
    val sessionLastLogTime = buffer.getLong(1)
    val logTime = input.getLong(0)
    if (logTime < sessionStartTime) {
      buffer.update(0, logTime)
    }
    if (logTime > sessionLastLogTime) {
      buffer.update(1, logTime)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val buffer1StartTime = buffer1.getLong(0)
    val buffer2StartTime = buffer2.getLong(0)
    if (buffer2StartTime < buffer1StartTime) {
      buffer1.update(0, buffer2StartTime)
    }
    val buffer1EndTime = buffer1.getLong(1)
    val buffer2EndTime = buffer2.getLong(1)
    if (buffer2EndTime > buffer1EndTime) {
      buffer1.update(1, buffer2EndTime)
    }
  }

  override def evaluate(buffer: Row): Any = {
    val sessionStartTime = buffer.getLong(0)
    val sessionLastLogTime = buffer.getLong(1)
    sessionLastLogTime - sessionStartTime
  }
}