package com.waitingforcode.sql.files_issues

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}

class IgnoreMissingFilesTest extends FlatSpec with Matchers {

  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL ignore missing files")
    .master("local[2]")
    .config("spark.sql.files.ignoreMissingFiles", "true")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  "ignore missing files flag" should "not make the processing fail when the files are deleted before processing" in {
    FilesManager.writeFiles()

    val linesCount = sparkSession.read
      .schema(StructType(Seq(StructField("letter", StringType))))
      .json(FilesManager.OutputDir)
      .filter(row => {
        FilesManager.removeFiles()
        println(s"filtering ${row}")
        // Returns always true, here just to start the files removal
        true
      })
      .count()

    linesCount should not equal 8
  }

}


object FilesManager {
  val OutputDir = "/tmp/spark-tests/invalid-files/missing-files"

  def removeFiles() = {
    synchronized {
      FileUtils.deleteDirectory(new File(OutputDir))
    }
  }

  def writeFiles() = {
    val dataToWrite = Seq(
      (Seq(
        """{"letter": "A", "number": 1}""",
        """{"letter": "B", "number": 2}"""), "1.jsonl"),
      (Seq(
        """{"letter": "C", "number": 3}""",
        """{"letter": "D", "number": 4}"""), "2.jsonl"),
      (Seq(
        """{"letter": "C", "number": 3}""",
        """{"letter": "D", "number": 4}"""), "3.jsonl"),
      (Seq(
        """{"letter": "C", "number": 3}""",
        """{"letter": "D", "number": 4}"""), "4.jsonl")
    )
    import scala.collection.JavaConverters._
    dataToWrite.foreach(linesToWriteWithFileName => {
      FileUtils.writeLines(new File(s"${OutputDir}/${linesToWriteWithFileName._2}"),
        linesToWriteWithFileName._1.asJava)
    })
  }
}
