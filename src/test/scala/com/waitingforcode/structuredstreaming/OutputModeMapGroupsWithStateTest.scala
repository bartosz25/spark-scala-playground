package com.waitingforcode.structuredstreaming

import com.waitingforcode.util.store.InMemoryKeyedStore
import com.waitingforcode.util.{InMemoryStoreWriter, NoopForeachWriter}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class OutputModeMapGroupsWithStateTest extends FlatSpec with Matchers with BeforeAndAfterAll {

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

  "append mode" should "not work for mapGroupWithState" in {
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val mappedValues =inputStream.toDS().toDF("id", "name")
      .groupByKey(row => row.getAs[Long]("id"))
      .mapGroupsWithState(timeoutConf = GroupStateTimeout.ProcessingTimeTimeout)(MappingFunction)
    inputStream.addData((1L, "test1"), (1L, "test2"), (2L, "test3") )

    val exception = intercept[AnalysisException] {
      mappedValues.writeStream.outputMode("append").foreach(new NoopForeachWriter[Seq[String]]()).start()
    }

    exception.message should include("mapGroupsWithState is not supported with Append output mode on a " +
      "streaming DataFrame/Dataset")
  }

  "complete mode" should "not work for mapGroupWithState" in {
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val mappedValues =inputStream.toDS().toDF("id", "name")
      .groupByKey(row => row.getAs[Long]("id"))
      .mapGroupsWithState(timeoutConf = GroupStateTimeout.ProcessingTimeTimeout)(MappingFunction)
    inputStream.addData((1L, "test1"), (1L, "test2"), (2L, "test3") )

    val exception = intercept[AnalysisException] {
      mappedValues.writeStream.outputMode("complete").foreach(new NoopForeachWriter[Seq[String]]()).start()
    }

    exception.message should include("Complete output mode not supported when there are no streaming " +
      "aggregations on streaming DataFrames/Datasets")
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
