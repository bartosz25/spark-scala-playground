package com.waitingforcode.sql

import com.waitingforcode.util.InMemoryDatabase
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

class ShuffleJoinTest extends FlatSpec with BeforeAndAfterAll with BeforeAndAfter with Matchers {
/*
  val sparkSession = SparkSession.builder()
    .appName("Spark shuffle join").master("local[*]")
    // The below 2 configs are mandatory to use broadcast join
    //.config("spark.sql.defaultSizeInBytes", "999") // @see -> org.apache.spark.sql.internal.SQLConf.DEFAULT_SIZE_IN_BYTES
    //.config("spark.sql.autoBroadcastJoinThreshold", "1111111111")
    .config("spark.sql.autoBroadcastJoinThreshold", "1")
    //.config("spark.sql.shuffle.partitions", "4")
    //.config("spark.sql.defaultSizeInBytes", "15")
    .config("spark.sql.join.preferSortMergeJoin", "false")
    .getOrCreate()
  // Structured data*/
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark shuffle join").master("local[*]")
    .config("spark.sql.autoBroadcastJoinThreshold", "1")
    .config("spark.sql.join.preferSortMergeJoin", "false")
    .getOrCreate()

  before {
    InMemoryDatabase.cleanDatabase()
  }

  override def afterAll() {
    InMemoryDatabase.cleanDatabase()
    sparkSession.stop()
  }

  "much smaller table" should "be joined with shuffle join" in {
    import sparkSession.implicits._
    val inMemoryCustomersDataFrame = Seq(
      (1, "Customer_1")
    ).toDF("id", "login")
    val inMemoryOrdersDataFrame = Seq(
      (1, 1, 50.0d, System.currentTimeMillis()), (2, 2, 10d, System.currentTimeMillis()),
      (3, 2, 10d, System.currentTimeMillis()), (4, 2, 10d, System.currentTimeMillis())
    ).toDF("id", "customers_id", "amount", "date")

    val ordersByCustomer = inMemoryOrdersDataFrame
      .join(inMemoryCustomersDataFrame, inMemoryOrdersDataFrame("customers_id") === inMemoryCustomersDataFrame("id"),
      "left")
    ordersByCustomer.foreach(customerOrder => {
        println("> " + customerOrder)
      })

    // shuffle join is executed because:
    // * the size of plan is greater than the size of broadcast join configuration (96  > 1):
    //   96 because: IntegerType (4) + IntegerType (4) + DoubleType (8) + LongType (8)) * 3 = 24 * 4 = 96)
    // * merge-sort join is disabled
    // * the join type is inner (supported by shuffle join)
    // * built hash table is smaller than the cost of broadcast (96 < 1 * 200, where 1 is spark.sql.autoBroadcastJoinThreshold
    //   and 200 is the default number of partitions)
    // * one of tables is at least 3 times smaller than the other (72 <= 96, where 72 is the size of customers
    //   table*3 and 96 is the total place taken by orders table)
    val queryExecution = ordersByCustomer.queryExecution.toString()
    println(s"> ${queryExecution}")
    queryExecution.contains("ShuffledHashJoin [customers_id#20], [id#5], LeftOuter, BuildRight") should be (true)
  }

  "when any of tables is at lest 3 times bigger than the other merge join" should "be prefered over shuffle join" in {
    // This situation is similar to the previous one
    // The difference is that the last column (timestamp) was removed from orders.
    // Because of that, the size of orders decreases to 96 - 4 * 8 = 64
    // Thus the criterion about the table at least 3 times bigger is not respected anymore.
    import sparkSession.implicits._
    val inMemoryCustomersDataFrame = Seq(
      (1, "Customer_1")
    ).toDF("id", "login")
    val inMemoryOrdersDataFrame = Seq(
      (1, 1, 50.0d), (2, 2, 10d), (3, 2, 10d), (4, 2, 10d)
    ).toDF("id", "customers_id", "amount")

    val ordersByCustomer = inMemoryOrdersDataFrame
      .join(inMemoryCustomersDataFrame, inMemoryOrdersDataFrame("customers_id") === inMemoryCustomersDataFrame("id"),
      "left")
    ordersByCustomer.foreach(customerOrder => {
      println("> " + customerOrder)
    })

    val queryExecution = ordersByCustomer.queryExecution.toString()
    println("> " + ordersByCustomer.queryExecution)
    queryExecution.contains("ShuffledHashJoin [customers_id#20], [id#5], LeftOuter, BuildRight") should be (false)
    queryExecution.contains("SortMergeJoin [customers_id#18], [id#5], LeftOuter") should be (true)
  }

  "sort merge join" should "be executed instead of shuffle when the data comes from relational database" in {
    InMemoryDatabase.cleanDatabase()
    JoinHelper.createTables()
    val customerIds = JoinHelper.insertCustomers(1)
    JoinHelper.insertOrders(customerIds, 4)
    val OptionsMap: Map[String, String] =
      Map("url" -> InMemoryDatabase.DbConnection, "user" -> InMemoryDatabase.DbUser, "password" -> InMemoryDatabase.DbPassword,
        "driver" ->  InMemoryDatabase.DbDriver)
    val customersJdbcOptions = OptionsMap ++ Map("dbtable" -> "customers")
    val customersDataFrame = sparkSession.read.format("jdbc")
      .options(customersJdbcOptions)
      .load()
    val ordersJdbcOptions = OptionsMap ++ Map("dbtable" -> "orders")
    val ordersDataFrame = sparkSession.read.format("jdbc")
      .options(ordersJdbcOptions)
      .load()

    val ordersByCustomer = ordersDataFrame
      .join(customersDataFrame, ordersDataFrame("customers_id") === customersDataFrame("id"), "left")
    ordersByCustomer.foreach(customerOrder => {
      println("> " + customerOrder.toString())
    })

    // As explained in the post, the size of plan data is much bigger
    // than accepted to make the shuffle join. It's because the default sizeInBytes
    // used by JDBCRelation that is the same as the one used by
    // org.apache.spark.sql.sources.BaseRelation.sizeInBytes:
    // def sizeInBytes: Long = sqlContext.conf.defaultSizeInBytes
    // Thus even if the size of our data is the same as in the first test where
    // shuffle join was used, it won't be used here.
    val queryExecution = ordersByCustomer.queryExecution.toString()
    println("> " + ordersByCustomer.queryExecution)
    queryExecution.contains("ShuffledHashJoin [customers_id#20], [id#5], LeftOuter, BuildRight") should be (false)
    queryExecution.contains("SortMergeJoin [customers_id#6], [id#0], LeftOuter") should be (true)
  }
}
