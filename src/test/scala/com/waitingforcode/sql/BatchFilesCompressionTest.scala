package com.waitingforcode.sql

import java.io.File
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{DirectoryFileFilter, FalseFileFilter}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.JavaConverters._

class BatchFilesCompressionTest extends FlatSpec with Matchers with BeforeAndAfter {

  before {
    val baseDir = new File(JobConfig.BaseDir)
    if (baseDir.exists()) FileUtils.forceDelete(new File(JobConfig.BaseDir))
    FileUtils.forceMkdir(new File(JobConfig.IntermediaryDir))
    FileUtils.forceMkdir(new File(JobConfig.StagingDir))
    FileUtils.forceMkdir(new File(JobConfig.FinalOutputDir))
  }

  "all produced files" should "be processed and some of them compressed" in {
    val maxExecutionTime = System.currentTimeMillis() + 40000L
    new Thread(new ArchivesProducer(JobConfig.StagingDir, maxExecutionTime)).start()

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Spark SQL files compression test")
      .master("local[2]").getOrCreate()
    val schema = StructType(Seq(StructField("id", IntegerType, false), StructField("time", LongType, false)))

    var batchId = 0
    while (System.currentTimeMillis() < maxExecutionTime) {
      // Move files from the previous generation to the final output - only when they have a valid size
      val checkTime = System.currentTimeMillis()
      val newCompressedFiles = FileUtils.listFiles(new File(s"${JobConfig.IntermediaryDir}"),
        Array("json"), true).asScala.toSeq
      val classifiedFiles = classifyFilesToFinalAndNot(checkTime, newCompressedFiles)

      moveBigEnoughFilesToFinalDirectory(classifiedFiles.getOrElse(true, Seq.empty))

      val newFilesToCompress = FileUtils.listFiles(new File(JobConfig.StagingDir), Array("json"), true).asScala.toSeq
      val filesToCompress = (classifiedFiles.getOrElse(false, Seq.empty) ++ newFilesToCompress)

      import sparkSession.implicits._
      val batchOutputDir = s"${JobConfig.IntermediaryDir}/${batchId}"
      val fileNamesToCompress = filesToCompress.map(file => file.getAbsolutePath)
      sparkSession.read.schema(schema).json(fileNamesToCompress: _*).withColumn("partition_column", $"id")
        .write.partitionBy("partition_column").mode(SaveMode.Overwrite).json(batchOutputDir)

      IdleStateController.removeTooOldPartitions(checkTime)
      updatePartitionsState(checkTime, batchOutputDir)
      deleteAlreadyProcessedFiles(filesToCompress)
      batchId += 1
    }

    val linesFromAllFiles = FileUtils.listFiles(new File(JobConfig.BaseDir), Array("json"), true).asScala
      .flatMap(file => {
        val fileJsonLines = FileUtils.readFileToString(file).split("\n")
        fileJsonLines
      })

    linesFromAllFiles.foreach(line => {
      Store.allSavedLines should contain(line)
    })
    linesFromAllFiles should have size Store.allSavedLines.size
    FileUtils.listFiles(new File(JobConfig.FinalOutputDir), null, true).asScala.nonEmpty shouldBe true
  }

  private def classifyFilesToFinalAndNot(checkTime: Long, filesToClassify: Seq[File]): Map[Boolean, Seq[File]] = {
    filesToClassify.groupBy(file => {
      file.length() >= JobConfig.OutputMinSize1KbInBytes ||
        IdleStateController.canBeTransformedToFinal(checkTime, file.getParentFile.getName)
    })
  }

  private def moveBigEnoughFilesToFinalDirectory(files: Seq[File]): Unit = {
    val allRenamed = files.forall(file => {
      file.renameTo(new File(s"${JobConfig.FinalOutputDir}/${System.nanoTime()}-${file.getName}"))
    })
    assert(allRenamed, "All files should be moved to the target directory but it was not the case")
  }

  private def updatePartitionsState(checkTime: Long, partitionsDir: String) = {
    val createdPartitions =
      FileUtils.listFilesAndDirs(new File(partitionsDir), FalseFileFilter.INSTANCE, DirectoryFileFilter.DIRECTORY).asScala
    createdPartitions
      .filter(partitionDir => partitionDir.getName.startsWith("partition_"))
      .foreach(partitionDir => {
        IdleStateController.updatePartition(checkTime, partitionDir.getName)
    })
  }

  private def deleteAlreadyProcessedFiles(files: Seq[File]) = {
    files.foreach(file => {
      if (!file.delete()) {
        throw new IllegalStateException(s"An error occurred during the delete of ${file.getName}. " +
          s"All files to delete were: ${files.map(file => file.getAbsolutePath).mkString(", ")}")
      }
    })
  }

}


object IdleStateController {

  private val MaxIdleTimeMillis = TimeUnit.MINUTES.toMillis(2)
  private var PartitionsWithTTL: Map[String, Long] = Map.empty

  def updatePartition(checkTime: Long, partition: String) = {
    if (!PartitionsWithTTL.contains(partition)) {
      PartitionsWithTTL += (partition -> (checkTime + MaxIdleTimeMillis))
    }
  }

  def canBeTransformedToFinal(checkTime: Long, partition: String) = {
    false // checkTime > PartitionsWithTTL(partition)
  }

  def removeTooOldPartitions(checkTime: Long) = {
    PartitionsWithTTL = PartitionsWithTTL.filter {
      case (_, expirationTime) => checkTime < expirationTime
    }
  }

}

object JobConfig {
  val BaseDir = "/tmp/archiver"
  val StagingDir = s"${BaseDir}/staging"
  val FinalOutputDir = s"${BaseDir}/test_output"
  val IntermediaryDir = s"${BaseDir}/test_temporary"
  val OutputMinSize1KbInBytes = 1000
}


class ArchivesProducer(outputDir: String, maxExecutionTime: Long) extends Runnable {
  override def run(): Unit = {
    while (System.currentTimeMillis() < maxExecutionTime - 5000) {
      val jsons = (0 to 10).map(message => s"""{"id":${message},"time":${System.currentTimeMillis()}}""")
      val jsonsRandom = (0 to ThreadLocalRandom.current().nextInt(10))
        .map(message => s"""{"id":${message},"time":${System.currentTimeMillis()+10}}""")

      Store.addLines(jsons)
      Store.addLines(jsonsRandom)
      FileUtils.write(new File(s"${outputDir}/${System.currentTimeMillis()}.json"), Seq(jsons.mkString("\n"),
        jsonsRandom.mkString("\n")).mkString("\n"))
      Thread.sleep(200)
    }
  }
}

object Store {
  private var jsonLines: Seq[String] = Seq.empty

  def addLines(newLines: Seq[String]): Unit = {
    jsonLines = jsonLines ++ newLines
  }

  def allSavedLines = jsonLines

}
