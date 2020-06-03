package com.waitingforcode.structuredstreaming

import java.io.File

import com.waitingforcode.util.store.InMemoryKeyedStore
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class FileSourceTest extends FlatSpec with Matchers with BeforeAndAfter {

  val streamDir = "/tmp/file_source"

  before {
    FileUtils.deleteQuietly(new File(streamDir))
  }

  "all files" should "be processed by file source" in {
    val dataDir = s"${streamDir}/data"
    new Thread(new Runnable {
      override def run(): Unit = {
        var nr = 1
        while (nr <= 5) {
          val fileName = s"${dataDir}/${nr}.txt"
          val jsonContent = nr.toString
          FileUtils.writeStringToFile(new File(fileName), jsonContent)
          nr += 1
          Thread.sleep(5000L)
        }
      }
    }).start()

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Spark Structured Streaming file source")
      .master("local[*]").getOrCreate()

    val inputData = sparkSession.readStream.textFile(dataDir)
      .select("value")

    val outputData = inputData.writeStream
      .option("checkpointLocation", "/tmp/file_source/checkpoint")
      .foreach(new ForeachWriter[Row] {
        override def open(partitionId: Long, epochId: Long): Boolean = true
        override def process(value: Row): Unit = {
          InMemoryKeyedStore.addValue(value.getAs[String]("value"), "ok")
        }
        override def close(errorOrNull: Throwable): Unit = {}
      })

    outputData.start().awaitTermination(60000)

    InMemoryKeyedStore.allValues should have size 5
    InMemoryKeyedStore.allValues.keys should contain allOf("1", "2", "3", "4", "5")
    println("Got >>> " + InMemoryKeyedStore.allValues)
  }

}

