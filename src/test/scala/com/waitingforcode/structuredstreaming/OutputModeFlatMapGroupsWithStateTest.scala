package com.waitingforcode.structuredstreaming

import java.sql.Timestamp

import com.waitingforcode.util.store.InMemoryKeyedStore
import com.waitingforcode.util.{InMemoryStoreWriter, NoopForeachWriter}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.GroupStateTimeout._
import org.apache.spark.sql.streaming.{GroupState, OutputMode}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class OutputModeFlatMapGroupsWithStateTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val sparkSession: SparkSession = SparkSession.builder().appName("Spark Structured Streaming output modes -" +
    "flatMapGroupsWithState")
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  private val MappingFunction: (Long, Iterator[Row], GroupState[Seq[String]]) => Iterator[String] = (key, values, state) => {
    val stateNames = state.getOption.getOrElse(Seq.empty)
    //state.setTimeoutDuration(1000L)
    val stateNewNames = stateNames ++ values.map(row => row.getAs[String]("name"))
    state.update(stateNewNames)
    Iterator(stateNewNames.mkString(","))
  }

  "multiple flatMapGroupsWithState" should "fail because they're not all in append mode" in {
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val flattenResults = inputStream.toDS().toDF("id", "name")
      .groupByKey(row => row.getAs[Long]("id"))
      .flatMapGroupsWithState(outputMode = OutputMode.Append(), timeoutConf = NoTimeout())(MappingFunction)
      .groupByKey(key => key)
      .flatMapGroupsWithState(outputMode = OutputMode.Update(),
        timeoutConf = NoTimeout())((key, values, state: GroupState[String]) => Iterator(""))
    inputStream.addData((1L, "test1"), (1L, "test2"), (2L, "test3"))

    val exception = intercept[AnalysisException] {
      flattenResults.writeStream.outputMode("complete").foreach(new NoopForeachWriter[String]()).start()
    }

    exception.message should include("Multiple flatMapGroupsWithStates are not supported when they are not " +
      "all in append mode or the output mode is not append on a streaming DataFrames/Datasets")
  }

  "flatMapGroupsWithState" should "fail for complete mode" in {
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)

    val exception = intercept[IllegalArgumentException] {
      inputStream.toDS().toDF("id", "name")
        .groupByKey(row => row.getAs[Long]("id"))
        .flatMapGroupsWithState(outputMode = OutputMode.Complete(),
          timeoutConf = NoTimeout())(MappingFunction)
    }

    exception.getMessage should include("The output mode of function should be append or update")
  }

  "flatMapGroupsWithState" should "fail for append mode in flatMap and update mode in sink" in {
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val flattenResults = inputStream.toDS().toDF("id", "name")
      .groupByKey(row => row.getAs[Long]("id"))
      .flatMapGroupsWithState(outputMode = OutputMode.Append(),
        timeoutConf = NoTimeout())(MappingFunction)
    inputStream.addData((1L, "test1"), (1L, "test2"), (2L, "test3"))

    val exception = intercept[AnalysisException] {
      flattenResults.writeStream.outputMode("update").foreach(new NoopForeachWriter[String]()).start()
    }

    exception.message should include("flatMapGroupsWithState in append mode is not supported with Update output " +
      "mode on a streaming DataFrame/Dataset")
  }

  "flatMapGroupsWithState" should "work for append mode in flatMap and sink" in {
    val testKey = "flatMapGroupsWithState-append-mode"
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val flattenResults = inputStream.toDS().toDF("id", "name")
      .groupByKey(row => row.getAs[Long]("id"))
      .flatMapGroupsWithState(outputMode = OutputMode.Append(),
        timeoutConf = NoTimeout())(MappingFunction)
    inputStream.addData((1L, "test10"), (1L, "test11"), (2L, "test20"))

    val query = flattenResults.writeStream.outputMode("append").foreach(
      new InMemoryStoreWriter[String](testKey, (state) => state)).start()
    new Thread(new Runnable() {
      override def run(): Unit = {
        while (!query.isActive) {}
        Thread.sleep(5000)
        inputStream.addData((1L, "test12"))
        inputStream.addData((1L, "test13"))
      }
    }).start()

    query.awaitTermination(45000)
    val savedValues = InMemoryKeyedStore.getValues(testKey)
    savedValues should have size 3
    savedValues should contain allOf("test20", "test10,test11", "test10,test11,test12,test13")
  }

  "flatMapGroupsWithState" should "fail for append mode in flatMap and complete mode in sink" in {
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val flattenResults = inputStream.toDS().toDF("id", "name")
      .groupByKey(row => row.getAs[Long]("id"))
      .flatMapGroupsWithState(outputMode = OutputMode.Append(),
        timeoutConf = NoTimeout())(MappingFunction)
    inputStream.addData((1L, "test1"), (1L, "test2"), (2L, "test3"))

    val exception = intercept[AnalysisException] {
      flattenResults.writeStream.outputMode("complete").foreach(new NoopForeachWriter[String]()).start()
    }

    exception.message should include("Complete output mode not supported when there are no streaming aggregations " +
      "on streaming DataFrames/Datasets")
  }

  "flatMapGroupsWithState with aggregation after mapping" should "fail for append because of missing watermark" in {
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val flattenResults = inputStream.toDS().toDF("id", "name")
      .groupByKey(row => row.getAs[Long]("id"))
      .flatMapGroupsWithState(outputMode = OutputMode.Append(),
        timeoutConf = NoTimeout())(MappingFunction)
      .agg(count("*").as("count"))
    inputStream.addData((1L, "test10"), (1L, "test11"), (2L, "test20"))

    val exception = intercept[AnalysisException] {
      flattenResults.writeStream.outputMode("append").foreach(new NoopForeachWriter[Row]).start()
    }

    exception.getMessage() should include("Append output mode not supported when there are streaming " +
      "aggregations on streaming DataFrames/DataSets without watermark")
  }

  "flatMapGroupsWithState with aggregation after mapping" should "succeed for append when watermark is defined" in {
    val inputStream = new MemoryStream[(Timestamp, Long, String)](1, sparkSession.sqlContext)
    val now = 5000
    val flattenResults = inputStream.toDS().toDF("created", "id", "name")
      .groupByKey(row => row.getAs[Timestamp]("id"))
      .flatMapGroupsWithState(outputMode = OutputMode.Append(),
        timeoutConf = NoTimeout())((key, values, state: GroupState[(Timestamp, String)]) => Iterator((new Timestamp(1000), "")))
      .toDF("created", "name")
      .withWatermark("created", "1 second")
      .groupBy("created")
      .agg(count("*"))
    inputStream.addData((new Timestamp(now), 1L, "test10"), (new Timestamp(now), 1L, "test11"),
      (new Timestamp(now), 2L, "test20"))

    flattenResults.writeStream.outputMode("append").foreach(new NoopForeachWriter[Row]).start()
  }

  "flatMapGroupsWithState with aggregation after mapping" should "fail for append when watermark is not defined" in {
    val inputStream = new MemoryStream[(Timestamp, Long, String)](1, sparkSession.sqlContext)
    val now = 5000
    val flattenResults = inputStream.toDS().toDF("created", "id", "name")
      .groupByKey(row => row.getAs[Timestamp]("id"))
      .flatMapGroupsWithState(outputMode = OutputMode.Append(),
        timeoutConf = NoTimeout())((key, values, state: GroupState[(Timestamp, String)]) => Iterator((new Timestamp(1000), "")))
      .toDF("created", "name")
      .groupBy("created")
      .agg(count("*"))
    inputStream.addData((new Timestamp(now), 1L, "test10"), (new Timestamp(now), 1L, "test11"),
      (new Timestamp(now), 2L, "test20"))

    val exception = intercept[AnalysisException] {
      flattenResults.writeStream.outputMode("append").foreach(new NoopForeachWriter[Row]).start()
    }

    exception.getMessage() should include("Append output mode not supported when there are streaming aggregations on " +
      "streaming DataFrames/DataSets without watermark")
  }

  "flatMapGroupsWithState with aggregation after" should "fail for update mode" in {
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val flattenResults = inputStream.toDS().toDF("id", "name")
      .groupByKey(row => row.getAs[Long]("id"))
      .flatMapGroupsWithState(outputMode = OutputMode.Update(),
      timeoutConf = NoTimeout())(MappingFunction)
      .agg(count("*"))
    inputStream.addData((1L, "test1"), (1L, "test2"), (2L, "test3"))

    val exception = intercept[AnalysisException] {
      flattenResults.writeStream.outputMode("update").foreach(new NoopForeachWriter[Row]()).start()
    }

    exception.message should include("flatMapGroupsWithState in update mode is not supported with " +
      "aggregation on a streaming DataFrame/Dataset")
  }

  "flatMapGroupsWithState without aggregation" should "be correctly executed in update mode" in {
    val testKey = "flatMapGroupsWithState-no-aggregation"
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val flattenResults = inputStream.toDS().toDF("id", "name")
      .groupByKey(row => row.getAs[Long]("id"))
      .flatMapGroupsWithState(outputMode = OutputMode.Update(),
        timeoutConf = NoTimeout())(MappingFunction)
    inputStream.addData((1L, "test10"), (1L, "test11"), (2L, "test20"))

    val query = flattenResults.writeStream.outputMode("update")
      .foreach(new InMemoryStoreWriter[String](testKey, (state) => state)).start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        while (!query.isActive) {}
        Thread.sleep(5000)
        inputStream.addData((1L, "test12"))
        inputStream.addData((1L, "test13"))
      }
    }).start()
    query.awaitTermination(40000)

    val savedValues = InMemoryKeyedStore.getValues(testKey)
    savedValues should have size 3
    savedValues should contain allOf("test20", "test10,test11", "test10,test11,test12,test13")
  }

}
