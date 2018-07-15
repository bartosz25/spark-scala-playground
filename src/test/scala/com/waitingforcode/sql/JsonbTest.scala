package com.waitingforcode.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}


class JsonbTest extends FlatSpec with Matchers with BeforeAndAfter {

  private val Connection = "jdbc:postgresql://127.0.0.1:5432/correlated_subqueries"
  private val User = "root"
  private val Password = "root"

  private val sparkSession: SparkSession = SparkSession.builder().appName("Spark SQL fetch size test").master("local[*]").getOrCreate()

  "wxcwxcwxc" should "dfsdfdsf" in {
    val opts = Map("url" -> "jdbc:postgresql://127.0.0.1:5432/correlated_subqueries",
      "dbtable" -> "jsonb_test", "user" -> "root", "password" -> "root",
      "driver" -> "org.postgresql.Driver")
    val schema = StructType(Seq(
      StructField("first_name", StringType, true), StructField("last_name", StringType, true)
    ))
    import sparkSession.implicits._
    val personDataFrame = sparkSession.read
      .format("jdbc")
      .options(opts)
      .load()
      .withColumn("person", functions.from_json($"content", schema))

    val extractedJsonNames = personDataFrame.collect.map(row => row.get(1).toString)

    extractedJsonNames should have size 1
    extractedJsonNames(0) shouldEqual "[X,Y]"
  }

}
