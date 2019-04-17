package com.waitingforcode.sql

import com.waitingforcode.util.InMemoryDatabase
import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class BroadcastHintTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    InMemoryDatabase.cleanDatabase()
    JoinHelper.createTables()
    val customerIds = JoinHelper.insertCustomers(1)
    JoinHelper.insertOrders(customerIds, 4)
  }

  override def afterAll() {
    InMemoryDatabase.cleanDatabase()
  }

  private val testSession = SparkSession.builder()
    .appName("Spark broadcast hint").master("local[*]")
    .getOrCreate()
  import testSession.implicits._

  "broadcast hint" should "be respected for left join" in {
    val inMemoryCustomersDataFrame = Seq(
      (1, "Customer_1")
    ).toDF("id", "login")
    val inMemoryOrdersDataFrame = Seq(
      (1, 1, 50.0d, System.currentTimeMillis()), (2, 2, 10d, System.currentTimeMillis()),
      (3, 2, 10d, System.currentTimeMillis()), (4, 2, 10d, System.currentTimeMillis())
    ).toDF("id", "customers_id", "amount", "date")

    val ordersByCustomer = functions.broadcast(inMemoryCustomersDataFrame)
      .join(inMemoryOrdersDataFrame, inMemoryOrdersDataFrame("customers_id") === inMemoryCustomersDataFrame("id"),
        "left")

    val queryExecution = ordersByCustomer.queryExecution.toString()
    queryExecution should include("ResolvedHint (broadcast)")
    ordersByCustomer.queryExecution.executedPlan.toString() should include("*(1) BroadcastHashJoin [id#5], [customers_id#19], LeftOuter, BuildRight")
  }

  "broadcast hint" should "not be respected for not supported join type" in {
    val inMemoryCustomersDataFrame = Seq(
      (1, "Customer_1")
    ).toDF("id", "login")
    val inMemoryOrdersDataFrame = Seq(
      (1, 1, 50.0d, System.currentTimeMillis()), (2, 2, 10d, System.currentTimeMillis()),
      (3, 2, 10d, System.currentTimeMillis()), (4, 2, 10d, System.currentTimeMillis())
    ).toDF("id", "customers_id", "amount", "date")

    val ordersByCustomer = functions.broadcast(inMemoryCustomersDataFrame)
      .join(inMemoryOrdersDataFrame, inMemoryOrdersDataFrame("customers_id") === inMemoryCustomersDataFrame("id"),
        "full_outer")

    val queryExecution = ordersByCustomer.queryExecution.toString()
    // The hint is still resolved at logical plan
    queryExecution should include("ResolvedHint (broadcast)")
    // But it's not respected at physical plan
    ordersByCustomer.queryExecution.executedPlan.toString() should not include("*(1) BroadcastHashJoin [id#5], [customers_id#19], LeftOuter, BuildRight")
    ordersByCustomer.queryExecution.executedPlan.toString() should include("SortMergeJoin [id#5], [customers_id#19], FullOuter")
  }

  "broadcast hint" should "be defined with SQL expression" in {
    Seq(
      (1, "Customer_1")
    ).toDF("id", "login").createTempView("customers")
    Seq(
      (1, 1, 50.0d, System.currentTimeMillis()), (2, 2, 10d, System.currentTimeMillis()),
      (3, 2, 10d, System.currentTimeMillis()), (4, 2, 10d, System.currentTimeMillis())
    ).toDF("id", "customers_id", "amount", "date").createTempView("orders")

    val query = testSession.sql("SELECT /*+ BROADCAST(c) */ * FROM customers c JOIN orders o ON o.customers_id = c.id")

    val queryExecution = query.queryExecution.toString()
    queryExecution should include("ResolvedHint (broadcast)")
    query.queryExecution.executedPlan.toString() should include("*(1) BroadcastHashJoin [id#5], [customers_id#19], Inner, BuildLeft")
  }


}
