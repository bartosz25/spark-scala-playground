package com.waitingforcode.structuredstreaming.foreachbatchsink

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{FlatSpec, Matchers}

class SideOutputTest extends FlatSpec with Matchers {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming side output")
    .master("local[*]").getOrCreate()
  import sparkSession.implicits._

  "foreachBatch" should "generate 2 outputs" in {
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
        dataset.persist()
        dataset.write.mode(SaveMode.Overwrite).json(s"/tmp/spark/side-output/json/batch_${batchId}")
        dataset.write.mode(SaveMode.Overwrite).parquet(s"/tmp/spark/side-output/parquet/batch_${batchId}")
        dataset.unpersist()
      })
      .start()

    query.awaitTermination(20000L)

    def jsonBatch(batchNr: Int) = s"/tmp/spark/side-output/json/batch_${batchNr}"
    val jsonFiles = FileUtils.getFile(new File("/tmp/spark/side-output/json")).listFiles().toSeq.map(file => file.getAbsolutePath)
    jsonFiles should contain allElementsOf (Seq(jsonBatch(0), jsonBatch(1), jsonBatch(2), jsonBatch(3),
      jsonBatch(4), jsonBatch(5), jsonBatch(6), jsonBatch(7), jsonBatch(8)))
    def parquetBatch(batchNr: Int) = s"/tmp/spark/side-output/parquet/batch_${batchNr}"
    val parquetFiles = FileUtils.getFile(new File("/tmp/spark/side-output/parquet")).listFiles.toSeq.map(file => file.getAbsolutePath)
    parquetFiles should contain allElementsOf(Seq(parquetBatch(0), parquetBatch(1), parquetBatch(2), parquetBatch(3),
      parquetBatch(4), parquetBatch(5), parquetBatch(6), parquetBatch(7), parquetBatch(8)))
  }

}
