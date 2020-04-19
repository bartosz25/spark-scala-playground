package com.waitingforcode.structuredstreaming

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class FileSinkManifestTest extends FlatSpec with Matchers with BeforeAndAfter {

  private val baseDir = "/tmp/files_sink_manifest"
  private val outputPath = s"${baseDir}/output"
  private val checkpointLocation = s"/${baseDir}/checkpoint"
  private val inputFilesDir = s"/${baseDir}/input"
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming file sink example")
    .master("local[2]").getOrCreate()

  before {
    FileUtils.deleteDirectory(new File(outputPath))
    FileUtils.deleteDirectory(new File(checkpointLocation))
    FileUtils.deleteDirectory(new File(inputFilesDir))
  }

  after {
    FileUtils.deleteDirectory(new File(inputFilesDir))
  }

  "file sink" should "write files and read from the file source" in {
    FileUtils.writeStringToFile(new File(s"${inputFilesDir}/${System.currentTimeMillis()}.json"),
      """{"letter": "a", "number": 1}""")
    FileUtils.writeStringToFile(new File(s"${inputFilesDir}/${System.currentTimeMillis()}.json"),
      """{"letter": "a", "number": 1}""")
    FileUtils.writeStringToFile(new File(s"${inputFilesDir}/${System.currentTimeMillis()}.json"),
      """{"letter": "a", "number": 1}""")
    FileUtils.writeStringToFile(new File(s"${inputFilesDir}/${System.currentTimeMillis()}.json"),
      """{"letter": "a", "number": 1}""")
    new Thread(new Runnable() {
      override def run(): Unit = {
        while (true) {
          FileUtils.writeStringToFile(new File(s"${inputFilesDir}/${System.currentTimeMillis()}.json"),
            """{"letter": "a", "number": 1}""")
          Thread.sleep(2000L)
        }
      }
    }).start()
    val schema = StructType(Array(
      StructField("letter", StringType), StructField("number", IntegerType)
    ))

    val writeQuery = sparkSession.readStream.schema(schema)
      .format("json")
      .load(s"file://${inputFilesDir}/*")
      .writeStream
      .format("json")
      .option("path", s"file://${outputPath}")
      .option("checkpointLocation", s"file://${checkpointLocation}")

    writeQuery.start().awaitTermination()
  }


}
