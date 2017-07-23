package com.waitingforcode.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class JoinTypesTest extends FlatSpec with BeforeAndAfter with Matchers {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark join types").master("local[*]")
    .config("spark.sql.crossJoin.enabled", "true")getOrCreate()


  import sparkSession.implicits._
  private val customers = Seq(
    (1, "Customer_1"), (2, "Customer_2"), (3, "Customer_3")
  ).toDF("id", "login")
  private val orders = Seq(
    (1, 1, 50.0d), (2, 2, 10d),
    (3, 2, 10d), (4, 2, 10d),
    (5, 1000, 19d)
  ).toDF("id", "customers_id", "amount")

  "inner join" should "collect only matching rows from both datasets" in {
    val ordersByCustomer = orders
      .join(customers, orders("customers_id") === customers("id"), "inner")
      .map(Mapper.mapJoinedRow(_))
      .collect()

    ordersByCustomer.size shouldEqual(4)
    ordersByCustomer should contain allOf("1,1,50.0,1,Customer_1", "2,2,10.0,2,Customer_2", "3,2,10.0,2,Customer_2",
      "4,2,10.0,2,Customer_2")
  }

  "cross join with enabled crossJoin property" should "create a Cartesian Product" in {
    val ordersByCustomer = orders.join(customers)
      .map(Mapper.mapJoinedRow(_))
      .collect()

    ordersByCustomer.size shouldEqual(15)
    ordersByCustomer should contain allOf("1,1,50.0,1,Customer_1", "1,1,50.0,2,Customer_2", "1,1,50.0,3,Customer_3",
      "2,2,10.0,1,Customer_1", "2,2,10.0,2,Customer_2", "2,2,10.0,3,Customer_3",
      "3,2,10.0,1,Customer_1", "3,2,10.0,2,Customer_2", "3,2,10.0,3,Customer_3",
      "4,2,10.0,1,Customer_1", "4,2,10.0,2,Customer_2", "4,2,10.0,3,Customer_3",
      "5,1000,19.0,1,Customer_1", "5,1000,19.0,2,Customer_2", "5,1000,19.0,3,Customer_3"
    )
  }

  "left outer join" should "collect all rows from the left dataset" in {
    val ordersByCustomer = orders
      .join(customers, orders("customers_id") === customers("id"), "leftouter")
      .map(Mapper.mapJoinedRow(_))
      .collect()

    ordersByCustomer.size shouldEqual(5)
    ordersByCustomer should contain allOf("1,1,50.0,1,Customer_1", "2,2,10.0,2,Customer_2", "3,2,10.0,2,Customer_2",
      "4,2,10.0,2,Customer_2", "5,1000,19.0,null,null")
  }

  "right outer join" should "collect all rows from the right dataset" in {
    val ordersByCustomer = orders
      .join(customers, orders("customers_id") === customers("id"), "rightouter")
      .map(Mapper.mapJoinedRow(_))
      .collect()

    ordersByCustomer.size shouldEqual(5)
    ordersByCustomer should contain allOf("null,null,null,3,Customer_3",
      "1,1,50.0,1,Customer_1", "2,2,10.0,2,Customer_2", "3,2,10.0,2,Customer_2",
      "4,2,10.0,2,Customer_2")
  }

  "full outer join" should "collect all rows from both datasets" in {
    val ordersByCustomer = orders
      .join(customers, orders("customers_id") === customers("id"), "fullouter")
      .map(Mapper.mapJoinedRow(_))
      .collect()

    ordersByCustomer.size shouldEqual(6)
    ordersByCustomer should contain allOf("null,null,null,3,Customer_3",
      "1,1,50.0,1,Customer_1", "2,2,10.0,2,Customer_2", "3,2,10.0,2,Customer_2",
      "4,2,10.0,2,Customer_2", "5,1000,19.0,null,null")
  }

  "left semi join" should "return only the matching rows with left table columns exclusively" in {
    val ordersByCustomer = orders
      .join(customers, orders("customers_id") === customers("id"), "leftsemi")
      .map(Mapper.halfMapJoinedRow(_))
      .collect()

    ordersByCustomer.size shouldEqual(4)
    ordersByCustomer should contain allOf("1,1,50.0", "2,2,10.0", "3,2,10.0", "4,2,10.0")
  }

  "left anti join" should "return only left dataset information without matching" in {
    val ordersByCustomer = orders
      .join(customers, orders("customers_id") === customers("id"), "leftanti")
      .map(Mapper.halfMapJoinedRow(_))
      .collect()

    ordersByCustomer.size shouldEqual(1)
    ordersByCustomer(0) shouldEqual("5,1000,19.0")
  }

}

object Mapper {

  def halfMapJoinedRow(row: Row): String = {
    val orderId = row.getInt(0)
    val orderCustomerId = row.getInt(1)
    val orderAmount =  row.getDouble(2)
    s"${orderId},${orderCustomerId},${orderAmount}"
  }

  def mapJoinedRow(row: Row): String = {
    val orderId = if (row.isNullAt(0)) null else row.getInt(0)
    val orderCustomerId = if (row.isNullAt(1)) null else row.getInt(1)
    val orderAmount = if (row.isNullAt(2)) null else row.getDouble(2)
    val customerId = if (row.isNullAt(3)) null else row.getInt(3)
    val customerName = if (row.isNullAt(4)) null else row.getString(4)
    s"${orderId},${orderCustomerId},${orderAmount},${customerId},${customerName}"
  }
}