package com.waitingforcode.tips

import com.waitingforcode.sql.JoinHelper
import com.waitingforcode.util.InMemoryDatabase
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

class TableOrViewNotFoundTest extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfter {

  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL nested data manipulation tip").master("local[*]").getOrCreate()

  before {
    JoinHelper.createTables()
  }

  after {
    InMemoryDatabase.cleanDatabase()
  }

  override def afterAll {
    sparkSession.stop()
  }

  "table" should "be found when it's registered explicitly" in {
    val customerIds = JoinHelper.insertCustomers(1)
    JoinHelper.insertOrders(customerIds, 4)
    val jdbcOptions =
      Map("url" -> InMemoryDatabase.DbConnection, "user" -> InMemoryDatabase.DbUser, "password" -> InMemoryDatabase.DbPassword,
        "driver" ->  InMemoryDatabase.DbDriver, "dbtable" -> "orders")
    val ordersDataFrame = sparkSession.read.format("jdbc")
      .options(jdbcOptions)
      .load()

    ordersDataFrame.createOrReplaceTempView("orders")
    val ordersAmounts = sparkSession.sqlContext.sql("SELECT amount FROM orders WHERE customers_id IS NOT NULL")

    val amounts = ordersAmounts.collect().map(row => (row.getAs[java.math.BigDecimal]("amount")))
    amounts should have size 4
    // amounts are set randomly, so not assert on them
  }

}
