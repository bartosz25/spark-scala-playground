package com.waitingforcode.structuredstreaming.initstate

import com.waitingforcode.util.InMemoryStoreWriter
import com.waitingforcode.util.store.InMemoryKeyedStore
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, OutputMode}
import org.scalatest.{FlatSpec, Matchers}

class KeyValueStoreTest extends FlatSpec with Matchers {

  val sparkSession = SparkSession.builder()
  .appName("Spark Structured Streaming initial state")
  .config("spark.sql.shuffle.partitions", 5)
  .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  private val MappingFunctionKeyValueLoad: (Long, Iterator[Row], GroupState[Seq[String]]) => Seq[String] = (key, values, state) => {
    val stateNames = state.getOption.getOrElse(KeyValueStore.State(key))
    val stateNewNames = stateNames ++ values.map(row => row.getAs[String]("name"))
    state.update(stateNewNames)
    stateNewNames
  }

  "the state" should "be loaded with key-value store" in {
    val testKey = "state-load-key-value"

    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    inputStream.addData((1L, "page1"), (2L, "page2"), (1L, "page3"))
    val initialDataset = inputStream.toDS().toDF("id", "name")
    val query = initialDataset.groupByKey(row => row.getAs[Long]("id"))
      .mapGroupsWithState(MappingFunctionKeyValueLoad)
      .writeStream
      .outputMode(OutputMode.Update())
      .foreach(new InMemoryStoreWriter[Seq[String]](testKey, (stateSeq) => stateSeq.mkString(", ")))
      .start()
    query.awaitTermination(60000)

    InMemoryKeyedStore.getValues(testKey) should have size 2
    InMemoryKeyedStore.getValues(testKey) should contain allOf("old_page1, page2",
      "old_page1, old_page2, page1, page3")
  }

}

object KeyValueStore {

  object State {
    private val Values = Map(
      1L -> Seq("old_page1", "old_page2"),
      2L -> Seq("old_page1")
    )

    def apply(key: Long) = Values(key)
  }

}
