package com.waitingforcode.structuredstreaming.initstate

import com.waitingforcode.util.InMemoryStoreWriter
import com.waitingforcode.util.store.InMemoryKeyedStore
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupState, OutputMode}
import org.scalatest.{FlatSpec, Matchers}

class JoinTest extends FlatSpec with Matchers {

  val sparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming initial state")
    .config("spark.sql.shuffle.partitions", 5)
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  private val MappingFunctionJoin: (Long, Iterator[Row], GroupState[Seq[String]]) => Seq[String] = (key, values, state) => {
    val materializedValues = values.toSeq
    val defaultState = materializedValues.headOption.map(row => Seq(row.getAs[String]("state_name"))).getOrElse(Seq.empty)
    val stateNames = state.getOption.getOrElse(defaultState)
    val stateNewNames = stateNames ++ materializedValues.map(row => row.getAs[String]("name"))
    state.update(stateNewNames)
    stateNewNames
  }

  "the state" should "be initialized with a join" in {
    val stateDataset = Seq((1L, "old_page1"), (2L, "old_page2")).toDF("state_id", "state_name")
    val testKey = "state-load-join"

    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    inputStream.addData((1L, "page1"), (2L, "page2"))

    val initialDataset = inputStream.toDS().toDF("id", "name")
    val joinedDataset =  initialDataset.join(stateDataset, $"id" === $"state_id", "left")
    val query = joinedDataset.groupByKey(row => row.getAs[Long]("id"))
      .mapGroupsWithState(MappingFunctionJoin)
      .writeStream
      .outputMode(OutputMode.Update())
      .foreach(new InMemoryStoreWriter[Seq[String]](testKey, (stateSeq) => stateSeq.mkString(", ")))
      .start()
    query.awaitTermination(60000)

    InMemoryKeyedStore.getValues(testKey) should have size 2
    InMemoryKeyedStore.getValues(testKey) should contain allOf("old_page2, page2", "old_page1, page1")
  }

}
