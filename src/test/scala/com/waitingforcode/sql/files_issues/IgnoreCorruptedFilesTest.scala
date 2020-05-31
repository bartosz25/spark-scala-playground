package com.waitingforcode.sql.files_issues

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import java.io.FileOutputStream

class IgnoreCorruptedFilesTest extends FlatSpec with Matchers with BeforeAndAfter {

  private val outputDir = "/tmp/spark-tests/invalid-files/corrupted-files"

  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL ignore corrupted files")
    .master("local[2]")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
  import sparkSession.implicits._
  before {
    Seq(("a"), ("b"), ("c"), ("d"), ("e"), ("f"),
      ("a"), ("b"), ("c"), ("d"), ("e"), ("f"),
      ("a"), ("b"), ("c"), ("d"), ("e"), ("f"),
      ("a"), ("b"), ("c"), ("d"), ("e"), ("f")
    ).toDS.toDF("letter")
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .json(outputDir)
  }

  behavior of "enabled ignoreCorruptFiles flag"

  it should "read JSON files as Parquet without exceptions" in {
    val parquetLinesCount = sparkSession.read
      .schema(StructType(
        Seq(StructField("letter", StringType))
      ))
      .parquet(outputDir).count()

    parquetLinesCount shouldEqual 0
  }

  it should "read altered GZIP compressed file without exceptions" in {
    import scala.collection.JavaConverters._
    val files = FileUtils.listFiles(new File(outputDir), TrueFileFilter.INSTANCE,
      TrueFileFilter.INSTANCE).asScala
    files.foreach(file => {
      if (file.getName.startsWith("part")) {
        println(s"Altering ${file.getName()}")
        val datFile = new FileOutputStream(file, true).getChannel
        // Let's truncate this file to make it impossible to decompress
        datFile.truncate(file.length() - 2)
        datFile.close()
      } else if (file.getName.startsWith(".part")) {
        // ignore checksum files to not use checksum to deduce file correctness
        file.delete()
      }
    })

    val inputData = sparkSession.read.schema(StructType(
      Seq(StructField("letter", StringType))
    )).json(outputDir)

    inputData.show()
    inputData.count() shouldEqual 24
  }

}
