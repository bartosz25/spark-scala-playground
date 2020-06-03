package com.waitingforcode.structuredstreaming

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

object FileSourceDemo extends App {

  val streamDir = "/tmp/file_source_demo"
  FileUtils.deleteQuietly(new File(streamDir))
  val dataDir = s"${streamDir}/data"
  val checkpointDir = s"${streamDir}/checkpoint"

  new Thread(new Runnable {
    override def run(): Unit = {
      val fileName = s"${dataDir}/1.txt"
      FileUtils.deleteQuietly(new File(fileName))
      var nr = 0
      while (true) {
        val jsonContent = nr.toString
        FileUtils.deleteQuietly(new File(fileName))
        FileUtils.writeStringToFile(new File(fileName), jsonContent)
        while (!(new File(s"/${checkpointDir}/sources/0/0").exists())) {
        }
        Thread.sleep(3000)
        nr += 1
      }
    }
  }).start()
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming file source")
    .master("local[*]").getOrCreate()

  val inputData = sparkSession.readStream.option("maxFileAge", "5s").textFile(dataDir)
    .select("value")


  val outputData = inputData.writeStream.option("checkpointLocation", checkpointDir)
    .format("console")

  outputData.start().awaitTermination(240000)
}
