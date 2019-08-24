package com.waitingforcode.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.debug
import org.scalatest.{FlatSpec, Matchers}

class WholeStageCodegenExecTest extends FlatSpec with Matchers {

  behavior of "WholeStageCodegen"

  it should "be generated for select, filter and map" in {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Spark SQL WholeStageCodegen enabled example").master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._
    val inMemoryCustomersDataFrame = Seq(
      (1, "Customer_1", true, "xxx"), (2, "Customer_2", true, "xxx"),
      (3, "Customer_3", true, "xxx"), (4, "Customer_4", false, "xxx"),
      (5, "Customer_5", false, "xxx"), (6, "Customer_6", false, "xxx"),
      (7, "Customer_7", true, "xxx"), (8, "Customer_8", true, "xxx")
    ).toDF("id", "login", "is_active", "useless_field")

    val activeUsers = inMemoryCustomersDataFrame
      .select("id", "login", "is_active")
      .filter($"is_active" === true)
      .map(row => {
        s"User ${row.getAs[String]("login")} is active :-)"
      })

    val wholeStageCodeGen = debug.codegenString(activeUsers.queryExecution.executedPlan)
    wholeStageCodeGen should include("Found 1 WholeStageCodegen subtrees.")
    println(wholeStageCodeGen)
  }

  it should "not generate 1 common function with disabed code generation" in {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Spark SQL WholeStageCodegen disabled example").master("local[*]")
      .config("spark.sql.codegen.wholeStage", false)
      .getOrCreate()
    import sparkSession.implicits._
    val inMemoryCustomersDataFrame = Seq(
      (1, "Customer_1", true, "xxx"), (2, "Customer_2", true, "xxx"),
      (3, "Customer_3", true, "xxx"), (4, "Customer_4", false, "xxx"),
      (5, "Customer_5", false, "xxx"), (6, "Customer_6", false, "xxx"),
      (7, "Customer_7", true, "xxx"), (8, "Customer_8", true, "xxx")
    ).toDF("id", "login", "is_active", "useless_field")

    val activeUsers = inMemoryCustomersDataFrame
      .select("id", "login", "is_active")
      .filter($"is_active" === true)
      .map(row => {
        s"User ${row.getAs[String]("login")} is active :-)"
      })

    val codeGen = debug.codegenString(activeUsers.queryExecution.executedPlan)
    codeGen should include("Found 0 WholeStageCodegen subtrees")
    println(codeGen)
  }

}
