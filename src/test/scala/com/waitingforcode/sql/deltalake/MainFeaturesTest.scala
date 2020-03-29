package com.waitingforcode.sql.deltalake

import java.io.File
import java.sql.Timestamp
import java.time.LocalDate

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class MainFeaturesTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val testedSparkSession: SparkSession = SparkSession.builder()
    .appName("Delta Lake main features test").master("local[*]").getOrCreate()
  import testedSparkSession.implicits._
  private val outputPath = "/tmp/delta_lake/main_features/data"


  override def beforeAll(): Unit = {
    if (false) {
      FileUtils.deleteDirectory(new File(outputPath))
      (1 to 10).foreach(nr => {
        val dataset = Seq((nr)).toDF("nr")
        dataset.write.format("delta").mode(SaveMode.Append).save(outputPath)
      })
    }
  }

  "version attribute" should "be used to retrieve past view of the data" in {
    // Let's get the dataset at its 3rd version
    val datasetVersion3 = testedSparkSession.read.format("delta")
      .option("versionAsOf", "3").load(outputPath)
      .map(row => row.getAs[Int]("nr")).collect()


    datasetVersion3 should have size 4
    datasetVersion3 should contain allOf(1, 2, 3, 4)
  }

  "timestamp" should "be associated to the version as processing time" in {
    val deltaTable = DeltaTable.forPath(testedSparkSession, outputPath)
    val fullHistoryDF = deltaTable.history()

    val deltaVersions = fullHistoryDF.map(row => row.getAs[Timestamp]("timestamp").toString).collect()

    deltaVersions.foreach(timestamp => timestamp should not be empty)
    // assert only on date to avoid hour issues (eg. versions written between xx:59 and yy:01)
    val expectedVersionDate = LocalDate.now.toString
    deltaVersions.foreach(timestamp => timestamp should startWith(expectedVersionDate))
  }

  "version 0" should "contain data written in the first write operation" in {
    val datasetVersion0 = testedSparkSession.read.format("delta")
      .option("versionAsOf", "0").load(outputPath)
      .map(row => row.getAs[Int]("nr")).collect()


    datasetVersion0 should have size 1
    datasetVersion0(0) shouldEqual 1
  }

  "the producer" should "not fail if the version doesn't exist" in {
    val rollbackVersion = 3
    val latestWrittenVersion = if (DeltaTable.isDeltaTable(testedSparkSession, outputPath)) {
      val deltaTable = DeltaTable.forPath(testedSparkSession, outputPath)
      deltaTable.history(1).map(row => row.getAs[Long]("version"))
        .collect()(0)
    } else {
      -1
    }

    println(s"latestWrittenVersion=${latestWrittenVersion}")
    val (previousData, saveMode) = if (latestWrittenVersion >= rollbackVersion) {
      // In that case we know we're reprocessing data because Airflow
      // Now we've 2 cases:
      if (0 == rollbackVersion) {
        // Rollback the first DAG run - we don't have the version -1
        (None, SaveMode.Overwrite)
      } else {
        // Rollback any previous DAG run - version-1
        val previousData = testedSparkSession.read.format("delta").option("path", outputPath)
          .option("versionAsOf", rollbackVersion-1).load()

        previousData.show(false)
        (Some(previousData), SaveMode.Overwrite)
      }
    } else {
      (None, SaveMode.Append)
    }

    val newData = Seq((10), (11), (12)).toDF("nr")

    val dataToWrite = previousData.map(data => data.union(newData)).getOrElse(newData)
    dataToWrite.show(false)
    dataToWrite.write.format("delta").mode(saveMode).save(outputPath)

    val deltaTable = DeltaTable.forPath(testedSparkSession, outputPath)
    deltaTable.history().show(false)

    val overwrittenData = testedSparkSession.read.format("delta").load(outputPath)
      .map(row => row.getAs[Int]("nr")).collect()
    overwrittenData should have size 3
    overwrittenData should contain allOf(10, 11, 12)
  }

}
