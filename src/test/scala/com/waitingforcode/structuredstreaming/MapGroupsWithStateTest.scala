package com.waitingforcode.structuredstreaming

import java.sql.Timestamp

import com.waitingforcode.util.store.InMemoryKeyedStore
import com.waitingforcode.util.{InMemoryStoreWriter, NoopForeachWriter}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, StreamingQueryException}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class MapGroupsWithStateTest extends FlatSpec with Matchers {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming output modes - mapGroupsWithState")
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  private val MappingFunction: (Long, Iterator[Row], GroupState[Seq[String]]) => Seq[String] = (key, values, state) => {
    val stateNames = state.getOption.getOrElse(Seq.empty)
    val stateNewNames = stateNames ++ values.map(row => row.getAs[String]("name"))
    state.update(stateNewNames)
    stateNewNames
  }

  private val MappingExpirationFunc: (Long, Iterator[Row], GroupState[Seq[String]]) => Seq[String] = (key, values, state) => {
    if (values.isEmpty && state.hasTimedOut) {
      Seq(s"${state.get}: timed-out")
    } else {
      val stateNames = state.getOption.getOrElse(Seq.empty)
      state.setTimeoutDuration(3000)
      val stateNewNames = stateNames ++ values.map(row => row.getAs[String]("name"))
      state.update(stateNewNames)
      stateNewNames
    }
  }

  "the state" should "expire after event time" in {
    val eventTimeExpirationFunc: (Long, Iterator[Row], GroupState[Seq[String]]) => Seq[String] = (key, values, state) => {
      if (values.isEmpty && state.hasTimedOut) {
        Seq(s"${state.get}: timed-out")
      } else {
        if (state.getOption.isEmpty) state.setTimeoutTimestamp(3000)
        val stateNames = state.getOption.getOrElse(Seq.empty)
        val stateNewNames = stateNames ++ values.map(row => row.getAs[String]("name"))
        state.update(stateNewNames)
        stateNewNames
      }
    }
    val now = 5000L
    val testKey = "mapGroupWithState-state-expired-event-time"
    val inputStream = new MemoryStream[(Timestamp, Long, String)](1, sparkSession.sqlContext)
    val mappedValues =inputStream.toDS().toDF("created", "id", "name")
      .withWatermark("created", "3 seconds")
      .groupByKey(row => row.getAs[Long]("id"))
      .mapGroupsWithState(timeoutConf = GroupStateTimeout.EventTimeTimeout())(eventTimeExpirationFunc)
    inputStream.addData((new Timestamp(now), 1L, "test10"),
      (new Timestamp(now), 1L, "test11"), (new Timestamp(now), 2L, "test20"),
      (new Timestamp(now+now), 3L, "test30"))

    val query = mappedValues.writeStream.outputMode("update")
      .foreach(new InMemoryStoreWriter[Seq[String]](testKey, (stateSeq) => stateSeq.mkString(","))).start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        while (!query.isActive) {}
        Thread.sleep(5000)
        // Now the watermark is about 7000 (10000 - 3000 ms)
        // So not only the values for the id=1 will be rejected but also its state
        // will expire
        inputStream.addData((new Timestamp(now), 1L, "test12"),
          (new Timestamp(now+now), 3L, "test31"))
      }
    }).start()


    query.awaitTermination(30000)

    val savedValues = InMemoryKeyedStore.getValues(testKey)
    savedValues should have size 6
    savedValues should contain allOf("test30", "test10,test11", "test20", "List(test10, test11): timed-out",
      "test30,test31", "List(test20): timed-out")
  }

  "the event-time state expiration" should "fail when the set timeout timestamp is earlier than the watermark" in {
    val eventTimeExpirationFunc: (Long, Iterator[Row], GroupState[Seq[String]]) => Seq[String] = (key, values, state) => {
      if (values.isEmpty && state.hasTimedOut) {
        Seq(s"${state.get}: timed-out")
      } else {
        state.setTimeoutTimestamp(2000)
        val stateNames = state.getOption.getOrElse(Seq.empty)
        val stateNewNames = stateNames ++ values.map(row => row.getAs[String]("name"))
        state.update(stateNewNames)
        stateNewNames
      }
    }
    val now = 5000L
    val testKey = "mapGroupWithState-event-time-state-expiration-failure"
    val inputStream = new MemoryStream[(Timestamp, Long, String)](1, sparkSession.sqlContext)
    val mappedValues =inputStream.toDS().toDF("created", "id", "name")
      .withWatermark("created", "2 seconds")
      .groupByKey(row => row.getAs[Long]("id"))
      .mapGroupsWithState(timeoutConf = GroupStateTimeout.EventTimeTimeout())(eventTimeExpirationFunc)
      inputStream.addData((new Timestamp(now), 1L, "test10"),
        (new Timestamp(now), 1L, "test11"), (new Timestamp(now), 2L, "test20"),
        (new Timestamp(now+now), 3L, "test30"))

    val exception = intercept[StreamingQueryException] {
      val query = mappedValues.writeStream.outputMode("update")
        .foreach(new InMemoryStoreWriter[Seq[String]](testKey, (stateSeq) => stateSeq.mkString(","))).start()

      new Thread(new Runnable() {
        override def run(): Unit = {
          while (!query.isActive) {}
          Thread.sleep(5000)
          // The watermark here is 8000. But in the eventTimeExpirationFunc we define the expiration timeout
          // to 3000. So obviously it makes the watermark condition from
          // org.apache.spark.sql.execution.streaming.GroupStateImpl.setTimeoutTimestamp fail
          inputStream.addData((new Timestamp(now), 1L, "test12"),
            (new Timestamp(now + now), 3L, "test31"))
        }
      }).start()

      query.awaitTermination(30000)
    }
    exception.getMessage should include("Timeout timestamp (2000) cannot be earlier than the current watermark (8000)")
  }

  "an event time expiration" should "not be executed when the watermark is not defined" in {
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val mappedValues =inputStream.toDS().toDF("id", "name")
      .groupByKey(row => row.getAs[Long]("id"))
      .mapGroupsWithState(timeoutConf = GroupStateTimeout.EventTimeTimeout())(MappingExpirationFunc)
    inputStream.addData((1L, "test10"), (1L, "test11"), (2L, "test20"), (3L, "test30"))

    val exception = intercept[AnalysisException] {
      mappedValues.writeStream.outputMode("update")
        .foreach(new NoopForeachWriter[Seq[String]]()).start()
    }
    exception.getMessage() should include("Watermark must be specified in the query using " +
      "'[Dataset/DataFrame].withWatermark()' for using event-time timeout in a [map|flatMap]GroupsWithState. " +
      "Event-time timeout not supported without watermark")
  }

  "a different state" should "be returned after the state expiration" in {
    val testKey = "mapGroupWithState-state-returned-after-expiration"
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val mappedValues = inputStream.toDS().toDF("id", "name")
      .groupByKey(row => row.getAs[Long]("id"))
      .mapGroupsWithState(timeoutConf = GroupStateTimeout.ProcessingTimeTimeout)(MappingExpirationFunc)
    inputStream.addData((1L, "test10"), (1L, "test11"), (2L, "test20"), (3L, "test30"))

    val query = mappedValues.writeStream.outputMode("update")
      .foreach(new InMemoryStoreWriter[Seq[String]](testKey, (stateSeq) => stateSeq.mkString(","))).start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        while (!query.isActive) {}
        Thread.sleep(5000)
        // In this batch we don't have the values for keys 1 and 3, thus both will
        // be returned as expired when this micro-batch will be processed
        // It's because the processing time timeout is of 3000 ms and here we wait 5000 ms
        // before restarting the processing
        inputStream.addData((2L, "test21"))
      }
    }).start()

    query.awaitTermination(30000)

    val savedValues = InMemoryKeyedStore.getValues(testKey)
    savedValues should have size 6
    savedValues should contain allOf("test10,test11", "test30", "test20", "List(test10, test11): timed-out",
      "List(test30): timed-out", "test20,test21")
  }

  "the state" should "not be discarded when there is no new data to process" in {
    val testKey = "mapGroupWithState-state-not-returned-after-expiration"
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val mappedValues =inputStream.toDS().toDF("id", "name")
      .groupByKey(row => row.getAs[Long]("id"))
      .mapGroupsWithState(timeoutConf = GroupStateTimeout.ProcessingTimeTimeout)(MappingExpirationFunc)
    inputStream.addData((1L, "test10"), (1L, "test11"), (2L, "test20"), (3L, "test30"))

    val query = mappedValues.writeStream.outputMode("update")
      .foreach(new InMemoryStoreWriter[Seq[String]](testKey, (stateSeq) => stateSeq.mkString(","))).start()

    query.awaitTermination(30000)

    val savedValues = InMemoryKeyedStore.getValues(testKey)
    // Here, unlike in the above test, only 3 values are returned. Since there is no new
    // micro-batch, the expired entries won't be detected as expired
    // It shows that the state execution depends on the data arrival
    savedValues should have size 3
    savedValues should contain allOf("test10,test11", "test30", "test20")
  }

  "append mode" should "be disallowed in mapGroupWithState" in {
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val mappedValues =inputStream.toDS().toDF("id", "name")
      .groupByKey(row => row.getAs[Long]("id"))
      .mapGroupsWithState(timeoutConf = GroupStateTimeout.ProcessingTimeTimeout)(MappingFunction)
    inputStream.addData((1L, "test10"), (1L, "test11"), (2L, "test20"), (3L, "test30"))

    val exception = intercept[AnalysisException] {
      mappedValues.writeStream.outputMode("append")
        .foreach(new NoopForeachWriter[Seq[String]]).start()
    }
    exception.getMessage() should include("mapGroupsWithState is not supported with Append output mode on a " +
      "streaming DataFrame/Dataset")
  }

  "update mode" should "work for mapGroupWithState" in {
    val testKey = "mapGroupWithState-update-output-mode"
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val mappedValues =inputStream.toDS().toDF("id", "name")
      .groupByKey(row => row.getAs[Long]("id"))
      .mapGroupsWithState(timeoutConf = GroupStateTimeout.ProcessingTimeTimeout)(MappingFunction)
    inputStream.addData((1L, "test10"), (1L, "test11"), (2L, "test20"), (3L, "test30"))

    val query = mappedValues.writeStream.outputMode("update")
      .foreach(new InMemoryStoreWriter[Seq[String]](testKey, (stateSeq) => stateSeq.mkString(","))).start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        while (!query.isActive) {}
        Thread.sleep(5000)
        inputStream.addData((1L, "test12"), (1L, "test13"), (2L, "test21"))
      }
    }).start()

    query.awaitTermination(30000)

    val savedValues = InMemoryKeyedStore.getValues(testKey)
    savedValues should have size 5
    savedValues should contain allOf("test30", "test10,test11", "test20", "test10,test11,test12,test13",
      "test20,test21")
  }

}
