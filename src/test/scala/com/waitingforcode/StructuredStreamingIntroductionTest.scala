package com.waitingforcode

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Paths

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Simple test showing processing with Spark's Structured Streaming. It simply
  * gets new entries from CSV files and aggregate them together to see
  * which one has the most occurrences.
  */
class StructuredStreamingIntroductionTest extends FlatSpec with Matchers with BeforeAndAfter {

  val sparkSession = SparkSession.builder().appName("Structured Streaming test")
    .master("local").getOrCreate()

  val csvDirectory = "/tmp/spark-structured-streaming-example"

  before {
    val path = Paths.get(csvDirectory)
    val directory = path.toFile
    directory.mkdir
    val files = directory.listFiles
    if (files != null) {
      files.foreach(_.delete)
    }
    addFile(s"${csvDirectory}/file_1.csv", "player_1;3\nplayer_2;1\nplayer_3;9")
    addFile(s"${csvDirectory}/file_2.csv", "player_1;3\nplayer_2;4\nplayer_3;5")
    addFile(s"${csvDirectory}/file_3.csv", "player_1;3\nplayer_2;7\nplayer_3;1")
  }

  private def addFile(fileName: String, content: String) = {
    val file = new File(fileName)
    file.createNewFile
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write(content)
    writer.close()
  }

  "CSV file" should "be consumed in Structured Spark Streaming" in {
    val csvEntrySchema = StructType(
      Seq(StructField("player", StringType, false),
      StructField("points", IntegerType, false))
    )

    // maxFilesPerTrigger = number of files - with that all files will be read at once
    // and the accumulator will store aggregated values for only 3 players
    val entriesDataFrame = sparkSession.readStream.option("sep", ";").option("maxFilesPerTrigger", "3")
      .option("basePath", csvDirectory)
      .schema(csvEntrySchema)
      .csv(csvDirectory)

    // Below processing first groups rows by field called 'player'
    // and after sum 'points' property of grouped rows
    val summedPointsByPlayer = entriesDataFrame.groupBy("player")
      .agg(sum("points").alias("collectedPoints"))

    val accumulator = sparkSession.sparkContext.collectionAccumulator[(String, Long)]
    val query:StreamingQuery = summedPointsByPlayer.writeStream
      .foreach(new ForeachWriter[Row]() {
        // true means that all partitions will be opened
        override def open(partitionId: Long, version: Long): Boolean = true

        override def process(row: Row): Unit = {
          println(s">> Processing ${row}")
          accumulator.add((row.getAs("player").asInstanceOf[String], row.getAs("collectedPoints").asInstanceOf[Long]))
        }

        override def close(errorOrNull: Throwable): Unit = {
          // do nothing
        }
      })
      .outputMode("complete")
      .start()

    // Shorter time can made that all files won't be processed
    query.awaitTermination(20000)

    accumulator.value.size shouldEqual 3
    accumulator.value should contain allOf (("player_1", 9), ("player_2", 12), ("player_3", 15))
  }


}

case class CsvEntry(player: String, points:Int)
