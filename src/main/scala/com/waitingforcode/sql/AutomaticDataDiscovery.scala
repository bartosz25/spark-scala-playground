package com.waitingforcode.sql

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SchemasMerger, SparkSession}

object AutomaticDataDiscovery extends App {

  val testDataset1 =
    """
      |{"letter": "A", "geo": {"city": "Paris", "country": "France"}}
      |{"number": 2, "geo": "Poland"}
      |{"letter": "C", "number": 3}
    """.stripMargin
  val testeDataset2 =
    s"""
       |{"letter": "d", "upper_letter": "D"}
    """.stripMargin
  val testeDataset3 =
    s"""
       |{"number": "four"}
    """.stripMargin

  FileUtils.writeStringToFile(new File("/tmp/input_json1.json"), testDataset1)
  FileUtils.writeStringToFile(new File("/tmp/input_json2.json"), testeDataset2)
  FileUtils.writeStringToFile(new File("/tmp/input_json3.json"), testeDataset3)

  val sparkSession: SparkSession = SparkSession.builder()
    .master("local[*]").appName("Spark Hive Example")
    .config("spark.sql.warehouse.dir", "/tmp/metastore")
    .enableHiveSupport().getOrCreate()

  val inputFileName = args(0)
  val tableName = args(1)

  val inputDataSchema = sparkSession.read.option("samplingRatio", "1.0")
    .json(inputFileName).schema
  val catalog = sparkSession.catalog

  val tableFinalSchema = if (catalog.tableExists(tableName)) {
    val existingSchema = sparkSession.sql(s"SELECT * FROM ${tableName}").schema
    val mergedSchema = SchemasMerger.merge(existingSchema, inputDataSchema)
    mergedSchema
  } else {
    inputDataSchema
  }

  import scala.collection.JavaConverters._
  sparkSession.sql(s"DROP TABLE IF EXISTS ${tableName}")
  catalog.createTable(tableName, "json", tableFinalSchema, Map.empty[String, String].asJava)

  catalog.listColumns(tableName).foreach(column => {
    println(s"Got=${column.name} (${column.dataType})")
  })
}
