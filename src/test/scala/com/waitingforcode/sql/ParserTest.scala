package com.waitingforcode.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{GreaterThan, Literal}
import org.scalatest.{FlatSpec, Matchers}

class ParserTest extends FlatSpec with Matchers {

  private val sparkSession: SparkSession = SparkSession.builder().appName("SQL parser test").master("local[*]")
    .getOrCreate()

  behavior of "parser"

  it should "convert SELECT into a LogicalPlan" in {
    val parsedPlan = sparkSession.sessionState.sqlParser.parsePlan("SELECT * FROM numbers WHERE nr > 1")

    parsedPlan.toString() should include ("'Project [*]")
    parsedPlan.toString() should include ("+- 'Filter ('nr > 1)")
    parsedPlan.toString() should include ("   +- 'UnresolvedRelation `numbers`")
  }

  it should "convert filter into an Expression" in {
    val parsedExpression = sparkSession.sessionState.sqlParser.parseExpression("nr > 1")

    parsedExpression shouldBe a [GreaterThan]
    parsedExpression.asInstanceOf[GreaterThan].left shouldBe a [UnresolvedAttribute]
    parsedExpression.asInstanceOf[GreaterThan].right shouldBe a [Literal]
  }

  it should "convert data type into Spark StructType" in {
    val parsedType = sparkSession.sessionState.sqlParser.parseDataType("struct<nr:int, letter:string>")

    parsedType.toString shouldEqual "StructType(StructField(nr,IntegerType,true), StructField(letter,StringType,true))"
  }


  it should "convert data type into a schema" in {
    val parsedSchema = sparkSession.sessionState.sqlParser.parseTableSchema("nr INTEGER, letter STRING")

    parsedSchema.toString shouldEqual "StructType(StructField(nr,IntegerType,true), StructField(letter,StringType,true))"
  }

}
