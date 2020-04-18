package com.waitingforcode.structuredstreaming

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class FileSinkTest extends FlatSpec with Matchers with BeforeAndAfter {

  private val outputPath = "/tmp/files_sink_test"
  private val checkpointLocation = "/tmp/file_sink_checkpoint"
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming file sink example")
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  before {
    FileUtils.deleteDirectory(new File(outputPath))
    FileUtils.deleteDirectory(new File(checkpointLocation))
  }

  after {
    FileUtils.deleteDirectory(new File(outputPath))
    FileUtils.deleteDirectory(new File(checkpointLocation))
  }

  "file sink" should "write files and the manifest" in {
    val inputStream = new MemoryStream[(Int, String)](1, sparkSession.sqlContext)
    inputStream.addData((1, "abc"), (2, "def"), (3, "ghi"))

    val writeQuery = inputStream.toDS().toDF("nr", "letters").writeStream.trigger(Trigger.Once())
      .format("json")
      .partitionBy("nr")
      .option("path", outputPath)
      .option("checkpointLocation", "/tmp/file_sink_checkpoint")

    writeQuery.start().awaitTermination()

    import scala.collection.JavaConverters._
    val writtenFilesAndDirs = FileUtils.listFilesAndDirs(new File(outputPath), TrueFileFilter.TRUE,
      TrueFileFilter.TRUE).asScala.map(file => file.getAbsolutePath).toSeq
    writtenFilesAndDirs should contain allOf(
      "/tmp/files_sink_test/nr=1",
      "/tmp/files_sink_test/nr=2",
      "/tmp/files_sink_test/nr=3",
      "/tmp/files_sink_test/_spark_metadata/0")
  }

}
