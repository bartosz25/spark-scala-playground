package com.waitingforcode

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CompressedFilesTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val conf = new SparkConf().setAppName("Spark compressed files test").setMaster("local[*]")
  private val sparkContext:SparkContext = SparkContext.getOrCreate(conf)

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
    val gzipPartitions = sparkContext.textFile(GzipPath.getAbsolutePath).getNumPartitions

    gzipPartitions shouldEqual 1
  }

  "splittable format" should "has more than 1 partition" in {
    val bzipPartitions = sparkContext.textFile(BzipPath.getAbsolutePath).getNumPartitions

    bzipPartitions > 1 shouldBe true
  }

}
