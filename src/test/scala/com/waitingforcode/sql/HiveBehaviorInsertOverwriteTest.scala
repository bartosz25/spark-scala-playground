package com.waitingforcode.sql

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class HiveBehaviorInsertOverwriteTest extends FlatSpec with Matchers {

  behavior of "Hive on Apache Spark SQL"

  private val TestSparkSession = SparkSession.builder().master("local").appName("Hive on Spark SQL - insertInto")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .enableHiveSupport().getOrCreate()
  import TestSparkSession.implicits._

  it should "write new data to DataFrame" in {
    val targetTable = "Hive_Respect_Schema"
    val initialDataFrame = Seq(("id#1", 3), ("id#2", 4), ("id#1", 6)).toDF("id", "value")

    initialDataFrame.write.mode(SaveMode.Overwrite).partitionBy("id").saveAsTable(targetTable)

    val dataFromTable = TestSparkSession.sql(s"SELECT * FROM ${targetTable}")

    val stringifiedDataFromTable = dataFromTable.map(row => HiveTestConverter.mapRowToString(row)).collect()
    stringifiedDataFromTable should have size 3
    stringifiedDataFromTable should contain allOf("id#1_3", "id#1_6", "id#2_4")
  }

  it should "ignore DataFrame schema when using insertInto command" in {
    val targetTable = "Hive_Ignore_Schema_1"
    val initialDataFrame = Seq(("id#1", 3), ("id#2", 4), ("id#1", 6)).toDF("id", "value")

    initialDataFrame.write.mode(SaveMode.Overwrite).partitionBy("id").saveAsTable(targetTable)

    val dataFromTable = TestSparkSession.sql(s"SELECT * FROM ${targetTable}")

    val stringifiedDataFromTable = dataFromTable.map(row => HiveTestConverter.mapRowToString(row)).collect()
    stringifiedDataFromTable should have size 3
    stringifiedDataFromTable should contain allOf("id#1_3", "id#1_6", "id#2_4")

    // Now we want to overwrite whole partition of 'id#1' but was you'll see, it doesn't work correctly
    Seq(("id#1", 120), ("id#1", 20)).toDF("id", "value").write.mode(SaveMode.Overwrite).insertInto(targetTable)

    val dataFromTableAfterInsert = TestSparkSession.sql(s"SELECT * FROM ${targetTable}")
    val stringifiedDataFromTableAfterInsert = dataFromTable.map(row => HiveTestConverter.mapRowToString(row)).collect()
    stringifiedDataFromTableAfterInsert should have size 5
    stringifiedDataFromTableAfterInsert should contain allOf("id#1_3", "id#1_6", "id#2_4", "120_null", "20_null")
  }

  it should "respect DataFrame schema because it fits to Hive table columns positions" in {
    val targetTable = "Hive_Respect_Schema_Index"
    val initialDataFrame = Seq(("id#1", 3), ("id#2", 4), ("id#1", 6)).toDF("id", "value")

    initialDataFrame.write.mode(SaveMode.Overwrite).partitionBy("id").saveAsTable(targetTable)

    val dataFromTable = TestSparkSession.sql(s"SELECT * FROM ${targetTable}")

    val stringifiedDataFromTable = dataFromTable.map(row => HiveTestConverter.mapRowToString(row)).collect()
    stringifiedDataFromTable should have size 3
    stringifiedDataFromTable should contain allOf("id#1_3", "id#1_6", "id#2_4")

    // DataFrame schema is "reversed" compared to the previous declaration
    Seq((120, "id#1"), (20, "id#1")).toDF("value", "id").write.mode(SaveMode.Overwrite).insertInto(targetTable)

    val dataFromTableAfterInsert = TestSparkSession.sql(s"SELECT * FROM ${targetTable}")
    val stringifiedDataFromTableAfterInsert = dataFromTable.map(row => HiveTestConverter.mapRowToString(row)).collect()
    stringifiedDataFromTableAfterInsert should have size 3
    stringifiedDataFromTableAfterInsert should contain allOf("id#1_120", "id#1_20", "id#2_4")
  }

  it should "respect DataFrame 3 fields schema because it fits to Hive table columns positions" in {
    val targetTable = "Hive_Respect_Schema_3_fields"
    val initialDataFrame = Seq(("id#1", 3, "a"), ("id#2", 4, "b"), ("id#1", 6, "c")).toDF("id", "value", "letter")

    initialDataFrame.write.mode(SaveMode.Overwrite).partitionBy("id").saveAsTable(targetTable)

    val dataFromTable = TestSparkSession.sql(s"SELECT * FROM ${targetTable}")

    val stringifiedDataFromTable = dataFromTable.map(row => HiveTestConverter.mapRowWithLetterToString(row)).collect()
    stringifiedDataFromTable should have size 3
    stringifiedDataFromTable should contain allOf("id#1_3_a", "id#1_6_c", "id#2_4_b")

    Seq((120, "d", "id#1"), (20, "e","id#1")).toDF("value", "letter", "id").write.mode(SaveMode.Overwrite).insertInto(targetTable)

    val dataFromTableAfterInsert = TestSparkSession.sql(s"SELECT * FROM ${targetTable}")
    val stringifiedDataFromTableAfterInsert = dataFromTable.map(row => HiveTestConverter.mapRowWithLetterToString(row)).collect()
    stringifiedDataFromTableAfterInsert should have size 3
    stringifiedDataFromTableAfterInsert should contain allOf("id#1_120_d", "id#1_20_e", "id#2_4_b")
  }
}

object HiveTestConverter {

  def mapRowToString(row: Row): String = {
    row.getAs[String]("id")+"_"+row.getAs[Int]("value")
  }

  def mapRowWithLetterToString(row: Row): String = {
    row.getAs[String]("id")+"_"+row.getAs[Int]("value")+"_"+row.getAs[String]("letter")
  }

}