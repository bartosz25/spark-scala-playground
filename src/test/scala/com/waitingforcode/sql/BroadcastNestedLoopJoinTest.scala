package com.waitingforcode.sql

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class BroadcastNestedLoopJoinTest extends FlatSpec with BeforeAndAfter with Matchers {

  private val SparkLocalSession = SparkSession.builder().appName("Correlated subquery test")
    .master("local[*]")
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()

  import SparkLocalSession.implicits._
  private val CustomersDataFrame = (1 to 2).map(id => (id, s"Customer#${id}")).toDF("customerId", "customerName")
  private val OrdersDataFrame = (1 to 3).map(orderId => (orderId, s"Order#${orderId}")).toDF("orderId", "orderName")

  behavior of "broadcast nested loop join"

  it should "be executed in cross join" in {
    val customersWithOrders = CustomersDataFrame.crossJoin(OrdersDataFrame)

    val mappedCustomersWithOrders =
      customersWithOrders.collect().map(row => s"${row.getAs[String]("customerName")};${row.getAs[String]("orderName")}")

    mappedCustomersWithOrders should have size 6
    mappedCustomersWithOrders should contain allOf("Customer#1;Order#1", "Customer#1;Order#2", "Customer#1;Order#3",
      "Customer#2;Order#1", "Customer#2;Order#2", "Customer#2;Order#3")
    // Whole physical plan looks like:
    // == Physical Plan ==
    //  BroadcastNestedLoopJoin BuildLeft, Cross
    // :- BroadcastExchange IdentityBroadcastMode
    //  :  +- LocalTableScan [customerId#5, customerName#6]
    // +- LocalTableScan [orderId#14, orderName#15]
    customersWithOrders.explain(true)
    val queryExecutionPlan = customersWithOrders.queryExecution.toString
    queryExecutionPlan should include("== Physical Plan ==\nBroadcastNestedLoopJoin BuildLeft, Cross")
  }

  it should "be executed in outer left join" in {
    val customersWithOrders = CustomersDataFrame.join(OrdersDataFrame, Seq.empty, "leftouter")

    val mappedCustomersWithOrders =
      customersWithOrders.collect().map(row => s"${row.getAs[String]("customerName")};${row.getAs[String]("orderName")}")

    mappedCustomersWithOrders should have size 6
    mappedCustomersWithOrders should contain allOf("Customer#1;Order#1", "Customer#1;Order#2", "Customer#1;Order#3",
      "Customer#2;Order#1", "Customer#2;Order#2", "Customer#2;Order#3")
    // Whole physical plan looks like:
    // == Physical Plan ==
    // BroadcastNestedLoopJoin BuildRight, LeftOuter
    // :- LocalTableScan [customerId#5, customerName#6]
    // +- BroadcastExchange IdentityBroadcastMode
    // +- LocalTableScan [orderId#14, orderName#15]
    val queryExecutionPlan = customersWithOrders.queryExecution.toString
    queryExecutionPlan should include("== Physical Plan ==\nBroadcastNestedLoopJoin BuildRight, LeftOuter")
  }

  it should "be executed in left semi join" in {
    val customersWithOrders = CustomersDataFrame.join(OrdersDataFrame, Seq.empty, "leftsemi")

    // Since semi-join includes only the columns from one side, the mapping here is different than in above code
    val mappedCustomersWithOrders =
      customersWithOrders.collect().map(row => s"${row.getAs[String]("customerName")}")

    mappedCustomersWithOrders should have size 2
    mappedCustomersWithOrders should contain allOf("Customer#1", "Customer#2")
    // Whole physical plan looks like:
    // == Physical Plan ==
    // BroadcastNestedLoopJoin BuildRight, LeftSemi
    // :- LocalTableScan [customerId#5, customerName#6]
    // +- BroadcastExchange IdentityBroadcastMode
    // +- LocalTableScan
    val queryExecutionPlan = customersWithOrders.queryExecution.toString
    queryExecutionPlan should include("== Physical Plan ==\nBroadcastNestedLoopJoin BuildRight, LeftSemi")
  }

  it should "be executed in left anti join" in {
    val customersWithOrders = CustomersDataFrame.join(OrdersDataFrame, Seq.empty, "leftanti")

    // The left anti join returns the rows from the left side without the corresponding
    // rows in the right side
    // However in this particular case (LeftSemi with BuildRight and without condition), Spark returns no rows
    // if (condition.isDefined) { // ... } else if (buildRows.nonEmpty == exists) { // ... }
    // else { Iterator.empty }
    // org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec#leftExistenceJoin
    val mappedCustomersWithOrders =
      customersWithOrders.collect().map(row => s"${row.getAs[String]("customerName")}")

    mappedCustomersWithOrders shouldBe empty
    // Whole physical plan looks like:
    // == Physical Plan ==
    // BroadcastNestedLoopJoin BuildRight, LeftAnti
    // :- LocalTableScan [customerId#5, customerName#6]
    // +- BroadcastExchange IdentityBroadcastMode
    // +- LocalTableScan
    val queryExecutionPlan = customersWithOrders.queryExecution.toString
    queryExecutionPlan should include("== Physical Plan ==\nBroadcastNestedLoopJoin BuildRight, LeftAnti")
  }

  it should "be executed in full outer join" in {
    val customersWithOrders = CustomersDataFrame.join(OrdersDataFrame, Seq.empty, "fullouter")

    val mappedCustomersWithOrders =
      customersWithOrders.collect().map(row => s"${row.getAs[String]("customerName")};${row.getAs[String]("orderName")}")

    mappedCustomersWithOrders should have size 6
    mappedCustomersWithOrders should contain allOf("Customer#1;Order#1", "Customer#1;Order#2", "Customer#1;Order#3",
      "Customer#2;Order#1", "Customer#2;Order#2", "Customer#2;Order#3")
    // Whole physical plan looks like:
    // == Physical Plan ==
    // BroadcastNestedLoopJoin BuildLeft, FullOuter
      // :- BroadcastExchange IdentityBroadcastMode
    //   :  +- LocalTableScan [customerId#5, customerName#6]
    // +- LocalTableScan [orderId#14, orderName#15]
    val queryExecutionPlan = customersWithOrders.queryExecution.toString
    queryExecutionPlan should include("== Physical Plan ==\nBroadcastNestedLoopJoin BuildLeft, FullOuter")
  }

  it should "not be executed when the size is bigger than the broadcast join threshold" in {
    val lotOfCustomersDataFrame = (1 to 440000).map(id => (id, s"Customer#${id}")).toDF("customerId", "customerName")
    val lotOfOrdersDataFrame = (1 to 440000).map(orderId => (orderId, s"Order#${orderId}")).toDF("orderId", "orderName")
    val customersWithOrders = lotOfCustomersDataFrame.crossJoin(lotOfOrdersDataFrame)
    // Here we don't collect all rows since it brings 193 600 000 000 rows that don't fit in memory
    // We only assert on the execution plan

    // Whole physical plan looks like:
    // == Physical Plan ==
    // CartesianProduct
    // :- LocalTableScan [customerId#23, customerName#24]
    // +- LocalTableScan [orderId#32, orderName#33]
    val queryExecutionPlan = customersWithOrders.queryExecution.toString
    queryExecutionPlan should include("== Physical Plan ==\nCartesianProduct")
  }
}
