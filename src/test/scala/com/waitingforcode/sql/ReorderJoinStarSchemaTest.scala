package com.waitingforcode.sql

import java.io.File

import com.waitingforcode.util.InMemoryLogAppender
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ReorderJoinStarSchemaTest extends FlatSpec with Matchers with BeforeAndAfterAll {


  private val enabled = "true"
  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("Reorder JOIN CBO star detection test")
    .config("spark.sql.cbo.joinReorder.enabled", s"${enabled}")
    .config("spark.sql.cbo.enabled",s"${enabled}")
    .config("spark.sql.cbo.joinReorder.dp.star.filter", s"${enabled}")
    .config("spark.sql.statistics.histogram.enabled", s"${enabled}")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.cbo.starSchemaDetection", enabled)
    .enableHiveSupport()
    .master("local[*]").getOrCreate()

  private val baseDir = "/tmp/cbo_star_schema/"

  override def beforeAll(): Unit = {
    val veryBigTable = "very_big_table"
    val bigTable = "big_table"
    val smallTable1 = "small_table1"
    val smallTable2 = "small_table2"
    val smallTable3 = "small_table3"
    val configs = Map(
      veryBigTable -> 5000,
      bigTable -> 1500,
      smallTable1 -> 800,
      smallTable2 -> 200,
      smallTable3 -> 300
    )
    configs.foreach {
      case (key, maxRows) => {
        val data = (1 to maxRows).map(nr => nr).mkString("\n")
        val dataFile = new File(s"${baseDir}${key}")
        FileUtils.writeStringToFile(dataFile, data)
        val id = s"${key}_id"
        sparkSession.sql(s"DROP TABLE IF EXISTS ${key}")
        sparkSession.sql(s"CREATE TABLE ${key} (${id} INT) USING hive OPTIONS (fileFormat 'textfile', fieldDelim ',')")
        sparkSession.sql(s"LOAD DATA LOCAL INPATH '${dataFile.getAbsolutePath}' INTO TABLE ${key}")
        sparkSession.sql(s"ANALYZE TABLE ${key} COMPUTE STATISTICS FOR COLUMNS ${id}")
      }
    }
  }

  override def afterAll(): Unit = {
    FileUtils.forceDelete(new File(baseDir))
  }

  "cost-based reorder join" should "apply to a not optimized query join" in {
    val logAppender = InMemoryLogAppender.createLogAppender(
      Seq("Applying Rule org.apache.spark.sql.catalyst.optimizer.CostBasedJoinReorder"))

    sparkSession.sql(
      """
        |SELECT vb.*, b.*, s1.*, s2.*
        |FROM very_big_table AS vb
        |JOIN big_table AS  b ON vb.very_big_table_id = b.big_table_id
        |JOIN small_table1 AS s1 ON vb.very_big_table_id = s1.small_table1_id
        |JOIN small_table2 AS s2 ON s1.small_table1_id = s2.small_table2_id
        |JOIN small_table3 AS s3 ON s2.small_table2_id = s3.small_table3_id
      """.stripMargin).explain(true)
    // ^---- have non star join

    logAppender.getMessagesText() should have size 1
    logAppender.getMessagesText()(0).trim should startWith("=== Applying Rule org.apache.spark.sql.catalyst.optimizer.CostBasedJoinReorder ===")
  }


}