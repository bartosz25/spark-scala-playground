package com.waitingforcode.sql.deltalake

import java.io.File

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * During the demo:
  * 1/ Generate 5 datasets:
  *    - 1 to 2 normally,
  *    - for 3 to 5 use an "XXXXX" username
  * 2/ Replace "XXXXX" by "user" and reprocess the versions 3 to 5
  * 3/ Replace "user" by "user2" and reprocess the versions 4 to 6
  * 4/ Replace "user2" by "user3" and reprocess the versions 5 to 6
  */
object OutputInvalidationPatternWithTimeTravel extends App {
  val userValue = "user2"
  (4 to 6).foreach(airflowVersionToWrite => {
    val logsDir = "/tmp/output_invalidation_patterns/time_travel/delta_lake"
    //val airflowVersionToWrite = 1 // args(0).toLong
    val lastWrittenAirflowVersion = AirflowVersion.lastVersion.getOrElse(-1L)

    val TestedSparkSession: SparkSession = SparkSession.builder()
      .appName("Delta Lake time travel with Airflow").master("local[*]").getOrCreate()
    import TestedSparkSession.implicits._
    val (previousDataset, saveMode) = if (lastWrittenAirflowVersion < airflowVersionToWrite) {
      (None, SaveMode.Append)
    } else {
      // We're backfilling, so get the last written Delta Lake version for
      // given Airflow version
      val versionToRestore = airflowVersionToWrite - 1
      val deltaVersionForAirflow = AirflowVersion.deltaVersionForAirflowVersion(versionToRestore)
      println(s"Backfilling from Airflow version=${versionToRestore} which uses ${deltaVersionForAirflow} Delta Lake version")
      (
        Some(
          TestedSparkSession.read.format("delta").option("versionAsOf", deltaVersionForAirflow).load(logsDir)
        ), SaveMode.Overwrite
      )
    }

    val newDataset = Seq(
      (s"v${airflowVersionToWrite}-${userValue}")
    ).toDF("user_id")

    val datasetToWrite = previousDataset.map(previousData => previousData.union(newDataset))
      .getOrElse(newDataset)

    println("Writing dataset")
    datasetToWrite.show(false)

    datasetToWrite.write.format("delta").mode(saveMode).save(logsDir)


    def getLastWrittenDeltaLakeVersion = {
      val deltaTable = DeltaTable.forPath(TestedSparkSession, logsDir)
      val maxVersionNumber = deltaTable.history().select("version")
        .agg(Map("version" -> "max")).first().getAs[Long](0)
      maxVersionNumber
    }

    val lastWrittenDeltaVersion = getLastWrittenDeltaLakeVersion
    AirflowVersion.writeDeltaVersion(airflowVersionToWrite, lastWrittenDeltaVersion)

  })

}

object AirflowVersion {

  private val DirPrefix = "/tmp/time_travel"
  private val LastVersionFile = new File(s"${DirPrefix}/last_version.txt")

  def lastVersion = {
    if (LastVersionFile.exists()) {
      Some(FileUtils.readFileToString(LastVersionFile).toLong)
    } else {
      None
    }
  }

  def deltaVersionForAirflowVersion(airflowVersion: Long): Long = {
    // No need to check the file existence. We're suppose to call this
    // method only for the backfilling scenario. Not having the check helps to
    // enforce this constraint
    FileUtils.readFileToString(versionFile(airflowVersion)).toLong
  }

  def writeDeltaVersion(airflowVersion: Long, deltaVersion: Long) = {
    FileUtils.writeStringToFile(LastVersionFile, airflowVersion.toString)
    // We make simple and store only the last DL version
    FileUtils.writeStringToFile(versionFile(airflowVersion), deltaVersion.toString)
  }

  private def versionFile(airflowVersion: Long) = new File(s"${DirPrefix}/${airflowVersion}.txt")
}

