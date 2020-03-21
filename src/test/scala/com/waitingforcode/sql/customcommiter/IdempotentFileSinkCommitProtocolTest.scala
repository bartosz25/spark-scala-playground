package com.waitingforcode.sql.customcommiter

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class IdempotentFileSinkCommitProtocolTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val outputDir = "/tmp/wfc/idempotent_file_sink"

  override def beforeAll(): Unit = {
    val dirToClean = new File(outputDir)
    if (dirToClean.isDirectory) {
      FileUtils.cleanDirectory(dirToClean)
    }
  }
  override def afterAll(): Unit = {
    beforeAll()
  }

  "custom commit protocol" should "always generate the same files even for append mode" in {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Spark SQL custom file commit protocol")
      .master("local[*]")
      .config("spark.sql.sources.commitProtocolClass",
        "com.waitingforcode.sql.customcommiter.IdempotentFileSinkCommitProtocol")
      .getOrCreate()

    import sparkSession.implicits._
    val dataset = Seq(
      (1, "a"), (1, "a"), (1, "a"), (2, "b"), (2, "b"), (3, "c"), (3, "c")
    ).toDF("nr", "letter")

    dataset.write.mode(SaveMode.Append).json(outputDir)
    import scala.collection.JavaConverters._
    val allFiles = FileUtils.listFiles(new File(outputDir), Array("json"), false).asScala
    allFiles should have size 4

    // Write it once again to ensure that there are still 4 files
    dataset.write.mode(SaveMode.Append).json(outputDir)
    val allFilesAfterRewrite = FileUtils.listFiles(new File(outputDir), Array("json"), false).asScala
    allFilesAfterRewrite should have size 4
  }

}

object EnvVariableSimulation {
  val JOB_ID = "file_committer_test"
}

object TestJobId extends App {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL custom file commit protocol")
    .master("local[*]")
    .getOrCreate()

  import sparkSession.implicits._
  val dataset = Seq(
    (1, "a"), (1, "a"), (1, "a"), (2, "b"), (2, "b"), (3, "c"), (3, "c")
  ).toDF("nr", "letter")

  dataset.write.mode(SaveMode.Append).json("/tmp/wfc/overridden_job_id")
}