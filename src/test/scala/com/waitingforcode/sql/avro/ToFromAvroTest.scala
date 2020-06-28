package com.waitingforcode.sql.avro

import java.io.EOFException

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class ToFromAvroTest extends FlatSpec with Matchers {

  private val OrderSchema =
    """
      |{
      |"type": "record",
      |"name": "order",
      |"fields": [
      | {"name": "id", "type": "int"},
      | {"name": "amount", "type": "double"}
      |]}
    """.stripMargin

  private val TestedSparkSession: SparkSession = SparkSession.builder()
    .appName("from_avro/to_avro test").master("local[*]").getOrCreate()
  import TestedSparkSession.implicits._

  "the schema mismatch" should "make the from_avro call fail" in {
    val ordersWithNull = Seq(
      ("test", ToFromAvroMyOrder(1, 30.33d)),
      ("test2", ToFromAvroMyOrder(2, null))
    ).toDF("col1", "col2")

    val parserError = intercept[EOFException] {
      ordersWithNull
        .select($"col1", org.apache.spark.sql.avro.to_avro($"col2").as("col_avro"))
        .select($"col1", org.apache.spark.sql.avro.from_avro($"col_avro", OrderSchema))
        .collect()
    }
    parserError.printStackTrace()
    parserError.getStackTrace.mkString("\n") should include("org.apache.avro.io.BinaryDecoder.readDouble")
    parserError.getStackTrace.mkString("\n") should include("org.apache.spark.sql.avro.AvroDataToCatalyst.nullSafeEval")

    val ordersWithoutNull = Seq(
      ("test", ToFromAvroMyOrder(1, 30.33d)),
      ("test2", ToFromAvroMyOrder(2, 10.10d))
    ).toDF("col1", "col2")
    val readOrders = ordersWithoutNull
      .select($"col1", org.apache.spark.sql.avro.to_avro($"col2").as("col_avro"))
      .select($"col1", org.apache.spark.sql.avro.from_avro($"col_avro", OrderSchema))
      .collect()

    readOrders should have size 2
  }

}


case class ToFromAvroMyOrder(id: Int, amount: java.lang.Double)