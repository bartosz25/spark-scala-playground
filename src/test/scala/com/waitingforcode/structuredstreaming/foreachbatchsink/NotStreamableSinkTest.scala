package com.waitingforcode.structuredstreaming.foreachbatchsink

import com.waitingforcode.util.store.InMemoryKeyedStore
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{FlatSpec, Matchers}

class NotStreamableSinkTest extends FlatSpec with Matchers {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming not streamable sink")
    .master("local[*]").getOrCreate()
  import sparkSession.implicits._

  "foreachBatch" should "save the data into a key-value memory store" in {
    val inputStream = new MemoryStream[Int](1, sparkSession.sqlContext)
    inputStream.addData(1, 2, 3, 4)
    new Thread(new Runnable() {
      override def run(): Unit = {
        while (true) {
          inputStream.addData(1, 2, 3, 4)
          Thread.sleep(1000L)
        }
      }
    }).start()
    val stream = inputStream.toDS().toDF("number")

    val query = stream.writeStream.trigger(Trigger.ProcessingTime(2000L))
      .foreachBatch((dataset, batchId) => {
        dataset.foreachPartition(rows => {
          rows.foreach(row => {
            InMemoryKeyedStore.addValue(s"batch_${batchId}_${row.getAs[Int]("number")}", "")
          })
        })
      })
      .start()

    query.awaitTermination(20000L)

    // According to the defined timeout, we should have at least 10 processed batches
    val distinctKeys = InMemoryKeyedStore.allValues.keys.map(key => key.dropRight(2)).toSeq.distinct
    distinctKeys should contain atLeastOneElementOf(Seq("batch_0", "batch_1", "batch_2", "batch_3", "batch_4", "batch_5",
      "batch_6", "batch_7", "batch_8"))
  }



}
