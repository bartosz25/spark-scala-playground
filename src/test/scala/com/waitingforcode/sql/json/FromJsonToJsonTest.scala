package com.waitingforcode.sql.json

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.{FlatSpec, Matchers}

class FromJsonToJsonTest extends FlatSpec with Matchers {

  private val TestedSparkSession: SparkSession = SparkSession.builder()
    .appName("from_json/to_json test").master("local[*]").getOrCreate()
  import TestedSparkSession.implicits._

  "nullable flag" should "not be respected" in {
    val ordersWithNull = Seq(
      ("test", ToFromJsonMyOrder(1, 30.33d, "a")),
      ("test2", ToFromJsonMyOrder(2, null, "b"))
    ).toDF("col1", "col2")


    val fromJsonSchema = new StructType()
      .add($"id".int.copy(nullable = false))
      .add($"amount".double.copy(nullable = false))
      .add($"letter".string.copy(nullable = false))

    // Even though the schema has not null fields, Apache Spark by default
    // will convert it into nullable ones when from_json is called
    // See org.apache.spark.sql.catalyst.expressions.JsonToStructs
    val collectedOrders = ordersWithNull
      .select($"col1", functions.to_json($"col2").as("col_json"))
      .select($"col1", functions.from_json($"col_json", fromJsonSchema).as("col"))
      .select("col.*")
      .as[(Int, java.lang.Double, String)]
      .collect()

    collectedOrders should have size 2
    collectedOrders should contain allOf((1, 30.33d, "a"), (2, null, "b"))
  }

  "a record not matching the schema" should "be returned as null" in {
    val fromJsonSchema = new StructType()
      .add($"number".int.copy(nullable = false))
      .add($"letter".string.copy(nullable = false))

    val inputData = Seq(
      ("""{"number": 1}"""),
      ("""{"letter": "A", "number": "two"}""")
    ).toDF("col_json")

    /**
      * Null is returned becuase of this block:
      * try {
      * converter(parser.parse(
      *         json.asInstanceOf[UTF8String],
      *         CreateJacksonParser.utf8String,
      * identity[UTF8String]))
      * } catch {
      * case _: BadRecordException => null
      * }
      *
      * See org.apache.spark.sql.catalyst.expressions.JsonToStructs
      */
    val unnestedData = inputData.select(functions.from_json($"col_json", fromJsonSchema).as("col"))
      .select("col.*")
      .map(row => row.toString)
      .collect()

    unnestedData should have size 2
    unnestedData should contain allOf("[1,null]", "[null,null]")
  }

}


case class ToFromJsonMyOrder(id: Int, amount: java.lang.Double, letter: String)