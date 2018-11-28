package com.waitingforcode

import java.io.File
import java.nio.file.Files

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

package object memoryimpact {

  val OneGbInBytes = 1073741824L

  private val File500Mb = "generated_file_500_mb.txt"

  private val File1Gb = "generated_file_1_gb.txt"

  private val File1Gb1Mb = "generated_file_1_gb_1mb.txt"

  private val File3Gb = "generated_file_3_gb.txt"

  private val testDirectory = "/tmp/spark-memory-impact"

  val Test500MbFile = s"${testDirectory}/${File500Mb}"

  val Test1GbFile = s"${testDirectory}/${File1Gb}"

  val Test1Gb1MbFile = s"${testDirectory}/${File1Gb1Mb}"

  val Test3GbFile = s"${testDirectory}/${File3Gb}"

  def sequentialProcessingSession = SparkSession.builder()
    .appName("Spark memory impact").master("local[1]")
    .getOrCreate()

  def parallelProcessingSession = SparkSession.builder()
    .appName("Spark memory impact").master("local[3]")
    .getOrCreate()

  def standaloneSession = SparkSession.builder()
    .appName("Spark memory impact").master("spark://localhost:7077")
    .config("spark.executor.memory", "900m")
    .config("spark.executor.core", "1")
    .config("spark.executor.extraClassPath", sys.props("java.class.path"))
    .getOrCreate()

  def processTextRdd(textRdd: RDD[String]) = {
    textRdd.map(txt => txt)
      .foreach(txt => {})
  }

  def processTextRddWithGroupBy(textRdd: RDD[String]) = {
    textRdd.map(txt => txt)
      .groupBy(key => key)
      .foreach(txt => {})
  }

  def prepareTestEnvironment() = {
    val testDirectoryPath = new File(testDirectory).toPath
    if (!Files.isDirectory(testDirectoryPath)) {
      Files.createDirectory(testDirectoryPath)
    }
    val file5000Path = new File(Test500MbFile).toPath
    if (!Files.exists(file5000Path)) {
      Files.copy(getClass.getResourceAsStream(s"/memory_impact/${File500Mb}"),
        file5000Path)
    }
    val file1GbPath = new File(Test1GbFile).toPath
    if (!Files.exists(file1GbPath)) {
      Files.copy(getClass.getResourceAsStream(s"/memory_impact/${File1Gb}"),
        file1GbPath)
    }
    val file3GbPath = new File(Test3GbFile).toPath
    if (!Files.exists(file3GbPath)) {
      Files.copy(getClass.getResourceAsStream(s"/memory_impact/${File3Gb}"),
        file3GbPath)
    }
    val file1Gb1MbPath = new File(Test1Gb1MbFile).toPath
    if (!Files.exists(file1Gb1MbPath)) {
      Files.copy(getClass.getResourceAsStream(s"/memory_impact/${File1Gb1Mb}"),
        file1Gb1MbPath)
    }
  }

}
