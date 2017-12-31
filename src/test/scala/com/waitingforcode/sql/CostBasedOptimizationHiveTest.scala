package com.waitingforcode.sql

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CostBasedOptimizationHiveTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder().appName("Cost-Based Optimizer test").master("local")
    .config("spark.sql.cbo.enabled", "true")
    .config("spark.sql.cbo.joinReorder.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()
  import sparkSession.implicits._
  
  private val HiveDataFile = new File("/tmp/hive_test.txt")

  override def beforeAll(): Unit = {
    Files.copy(getClass.getResourceAsStream("/hive_source.txt"), HiveDataFile.toPath)
  }
  
  override def afterAll() {
    sparkSession.stop()
    HiveDataFile.delete()
  }

  "CBO" should "be used when the ANALYZE command is executed" in {
    initializeContext(true)

    executeTheQuery.explain(true)
  }

  "CBO" should "not be applied without ANALYZE command" in {
    initializeContext(false)

    executeTheQuery.explain(true)
  }

  private def executeTheQuery(): DataFrame = {
    val mapToJoin = sparkSession.sql("SELECT * FROM map2")
    sparkSession.sql("SELECT key1, value1 FROM map1 WHERE key1 > 100")
      .join(mapToJoin, $"key1" === $"key2")
  }

  private def initializeContext(analyzeTable: Boolean): Unit = {
    for (i <- 1 to 2) {
      sparkSession.sql(s"DROP TABLE IF EXISTS map${i}")
      sparkSession.sql(s"CREATE TABLE map${i} (key${i} INT, value${i} STRING, upper_value${i} STRING) USING hive OPTIONS (fileFormat 'textfile', fieldDelim ',')")
      sparkSession.sql(s"LOAD DATA LOCAL INPATH '${HiveDataFile.getAbsolutePath}' INTO TABLE map${i}")
      if (analyzeTable) sparkSession.sql(s"ANALYZE TABLE map${i} COMPUTE STATISTICS")
    }
  }

}