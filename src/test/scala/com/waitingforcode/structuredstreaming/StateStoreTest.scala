package com.waitingforcode.structuredstreaming

import com.waitingforcode.util.{InMemoryLogAppender, InMemoryStoreWriter}
import com.waitingforcode.util.store.InMemoryKeyedStore
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{FlatSpec, Matchers}

class StateStoreTest extends FlatSpec with Matchers {

  val sparkSession: SparkSession = SparkSession.builder().appName("Spark Structured Streaming state store")
    .master("local[2]").getOrCreate()
  import sparkSession.sqlContext.implicits._

  "stateful count aggregation" should "use state store" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("Retrieved version",
      "Reported that the loaded instance StateStoreId", "Committed version"))
    val testKey = "stateful-aggregation-count-state-store-use"
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val aggregatedStream = inputStream.toDS().toDF("id", "name")
      .groupBy("id")
      .agg(count("*"))
    inputStream.addData((1, "a1"), (1, "a2"), (2, "b1"),
      (2, "b2"), (2, "b3"), (2, "b4"), (1, "a3"))

    val query = aggregatedStream.writeStream.trigger(Trigger.ProcessingTime(1000)).outputMode("update")
      .foreach(
        new InMemoryStoreWriter[Row](testKey, (row) => s"${row.getAs[Long]("id")} -> ${row.getAs[Long]("count(1)")}"))
      .start()

    query.awaitTermination(15000)

    val readValues = InMemoryKeyedStore.getValues(testKey)
    readValues should have size 2
    readValues should contain allOf("1 -> 3", "2 -> 4")
    // The assertions below show that the state is involved in the execution of the aggregation
    // The commit messages are the messages like:
    //  Committed version 1 for HDFSStateStore[id=(op=0,part=128),
    // dir=/tmp/temporary-6cbcad4e-70aa-4691-916c-cfccc842716b/state/0/128] to file
    // /tmp/temporary-6cbcad4e-70aa-4691-916c-cfccc842716b/state/0/128/1.delta
    val commitMessages = logAppender.getMessagesText().filter(_.startsWith("Committed version"))
    commitMessages.filter(_.startsWith("Committed version 1 for HDFSStateStore")).nonEmpty shouldEqual(true)
    // Retrieval messages look like:
    // version 0 of HDFSStateStoreProvider[id = (op=0, part=2), dir =
    // /tmp/temporary-cb59691c-21dc-4b87-9d76-de108ab32778/state/0/2] for update
    // (org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider:54)
    // It proves that the state is updated (new state is stored when new data is processed)
    val retrievalMessages = logAppender.getMessagesText().filter(_.startsWith("Retrieved version"))
    retrievalMessages.filter(_.startsWith("Retrieved version 0 of HDFSStateStoreProvider")).nonEmpty shouldEqual(true)
    // The report messages show that the state is physically loaded. An example of the message looks like:
    // Reported that the loaded instance StateStoreId(/tmp/temporary-6cbcad4e-70aa-4691-916c-cfccc842716b/state,0,3)
    // is active (org.apache.spark.sql.execution.streaming.state.StateStore:58)
    val reportMessages = logAppender.getMessagesText().filter(_.startsWith("Reported that the loaded instance"))
    reportMessages.filter(_.endsWith("state,0,1) is active")).nonEmpty shouldEqual(true)
    reportMessages.filter(_.endsWith("state,0,2) is active")).nonEmpty shouldEqual(true)
    reportMessages.filter(_.endsWith("state,0,3) is active")).nonEmpty shouldEqual(true)
  }

  "stateless count aggregation" should "not use state store" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("Retrieved version",
      "Reported that the loaded instance StateStoreId", "Committed version"))
    val data = Seq((1, "a1"), (1, "a2"), (2, "b1"), (2, "b2"), (2, "b3"), (2, "b4"), (1, "a3")).toDF("id", "name")

    val statelessAggregation = data.groupBy("id").agg(count("*").as("count")).collect()

    val mappedResult = statelessAggregation.map(row => s"${row.getAs[Int]("id")} -> ${row.getAs[Long]("count")}").toSeq
    mappedResult should have size 2
    mappedResult should contain allOf("1 -> 3", "2 -> 4")
    logAppender.getMessagesText() shouldBe empty
  }

}
