package com.waitingforcode.sql

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CompressedFilesTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val ProcessingSparkSession = SparkSession.builder()
    .appName("Spark SQL compression test").master("local[*]")
    // Defining this option is mandatory. The file is much smaller than the default value and it always ends up with
    // a single partition - even though the bzip2 is splittable
    .config("spark.sql.files.maxPartitionBytes", "5")
    .getOrCreate()

  private val BzipPath = new File("/tmp/bzip_test")
  private val GzipPath = new File("/tmp/gzip_test")

  override def beforeAll() {
    Files.createDirectory(BzipPath.toPath)
    Files.createDirectory(GzipPath.toPath)
    Files.copy(getClass.getResourceAsStream("/spark_compression/bzip_compression1.bz2"),
      new File(s"${BzipPath}/bzip_compression1.bz2").toPath)
    Files.copy(getClass.getResourceAsStream("/spark_compression/gzip_compression1.gz"),
      new File(s"${GzipPath}/gzip_compression1.gz").toPath)
  }

  override def afterAll(): Unit = {
    FileUtils.forceDelete(BzipPath)
    FileUtils.forceDelete(GzipPath)
  }

  "not splittable format" should "has only 1 partition" in {
    val gzipPartitions = ProcessingSparkSession.read.option("compression", "gzip")
      .format("text").load(GzipPath.getAbsolutePath+"/gzip_compression1.gz").rdd.getNumPartitions

    gzipPartitions shouldEqual 1
  }

  "splittable format" should "has more than 1 partition" in {
    val bzipPartitions = ProcessingSparkSession.read.option("compression", "bzip2")
      .format("text")
      .load(BzipPath.getAbsolutePath+"/bzip_compression1.bz2").rdd.getNumPartitions

    bzipPartitions shouldEqual 9
  }

}
