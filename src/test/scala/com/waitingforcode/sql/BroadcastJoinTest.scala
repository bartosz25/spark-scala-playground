package com.waitingforcode.sql

import com.waitingforcode.util.InMemoryDatabase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}
import org.apache.spark.sql.functions.broadcast

class BroadcastJoinTest extends FlatSpec with BeforeAndAfterAll with BeforeAndAfter with Matchers {

  override def beforeAll(): Unit = {
    InMemoryDatabase.cleanDatabase()
    JoinHelper.createTables()
    val customerIds = JoinHelper.insertCustomers(1)
    JoinHelper.insertOrders(customerIds, 4)
  }

  override def afterAll() {
    InMemoryDatabase.cleanDatabase()
  }

  "joined dataset" should "be broadcasted when it's smaller than the specified threshold" in {
    val sparkSession: SparkSession = createSparkSession(Int.MaxValue)
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

    val queryExecution = ordersByCustomer.queryExecution.toString()
    println(s"> ${queryExecution}")
    queryExecution.contains("BroadcastHashJoin [customers_id#20], [id#5], LeftOuter, BuildRight") should be (true)
    sparkSession.stop()
  }

  "joined dataset" should "not be broadcasted because the threshold was exceeded" in {
    val sparkSession: SparkSession = createSparkSession(10)
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

    val queryExecution = ordersByCustomer.queryExecution.toString()
    println(s"> ${queryExecution}")
    queryExecution.contains("BroadcastHashJoin [customers_id") should be (false)
    queryExecution.contains("ShuffledHashJoin [customers_id") should be (true)
    sparkSession.stop()
  }

  "broadcast join" should "be executed when data comes from RDBMS and the default size in bytes is smaller " +
    "than broadcast threshold" in {
    val sparkSession: SparkSession = createSparkSession(Int.MaxValue, 20L)

    val customersDataFrame = getH2DataFrame("customers", sparkSession)
    val ordersDataFrame = getH2DataFrame("orders", sparkSession)

    val ordersByCustomer = ordersDataFrame
      .join(customersDataFrame, ordersDataFrame("customers_id") === customersDataFrame("id"), "left")
    ordersByCustomer.foreach(customerOrder => {
      println("> " + customerOrder.toString())
    })

    // Here we expect broadcast join. It's because the default size of RDBMS datasource was
    // set to quite small number (20) and the condition of joined table size < broadcast threshold
    // is respected
    val queryExecution = ordersByCustomer.queryExecution.toString()
    println("> " + ordersByCustomer.queryExecution)
    // Do not assert on more than the beginning of the join.
    // Sometimes the ids after customers_id can be different
    queryExecution.contains("*BroadcastHashJoin [customers_id") should be (true)
    queryExecution.contains("SortMergeJoin [customers_id#6], [id#0], LeftOuter") should be (false)
  }

  "sort merge join" should "be executed instead of broadcast when the RDBMS default size is much bigger than" +
    "broadcast threshold" in {
    val sparkSession: SparkSession = createSparkSession(Int.MaxValue)

    val customersDataFrame = getH2DataFrame("customers", sparkSession)
    val ordersDataFrame = getH2DataFrame("orders", sparkSession)

    // Here the default size of RDBMS datasource is Long.MaxValue.
    // It means that we expect the data be too big to broadcast. Instead, it'll be
    // joined with sort-merge join.
    val ordersByCustomer = ordersDataFrame
      .join(customersDataFrame, ordersDataFrame("customers_id") === customersDataFrame("id"), "left")
    ordersByCustomer.foreach(customerOrder => {
      println("> " + customerOrder.toString())
    })

    val queryExecution = ordersByCustomer.queryExecution.toString()
    println("> " + ordersByCustomer.queryExecution)
    queryExecution.contains("*BroadcastHashJoin [customers_id#6], [id#0], LeftOuter, BuildRight") should be (false)
    queryExecution.contains("SortMergeJoin [customers_id") should be (true)
  }

  "broadcast join" should "be executed when broadcast hint is defined -" +
    "even if the RDBMS default size is much bigger than broadcast threshold" in {
    val sparkSession: SparkSession = createSparkSession(Int.MaxValue)

    val customersDataFrame = getH2DataFrame("customers", sparkSession)
    val ordersDataFrame = getH2DataFrame("orders", sparkSession)

    // Here the default size of RDBMS datasource is Long.MaxValue.
    // But we explicitly tells Spark to use broadcast join
    val ordersByCustomer = ordersDataFrame
      .join(broadcast(customersDataFrame), ordersDataFrame("customers_id") === customersDataFrame("id"), "left")
    ordersByCustomer.foreach(customerOrder => {
      println("> " + customerOrder.toString())
    })

    val queryExecution = ordersByCustomer.queryExecution.toString()
    println("> " + ordersByCustomer.queryExecution)
    queryExecution.contains("*BroadcastHashJoin [customers_id") should be (true)
    queryExecution.contains("+- BroadcastHint") should be (true)
    queryExecution.contains("SortMergeJoin [customers_id") should be (false)
  }

  private def getH2DataFrame(tableName: String, sparkSession: SparkSession): DataFrame = {
    val OptionsMap: Map[String, String] =
      Map("url" -> InMemoryDatabase.DbConnection, "user" -> InMemoryDatabase.DbUser, "password" -> InMemoryDatabase.DbPassword,
        "driver" ->  InMemoryDatabase.DbDriver)
    val jdbcOptions = OptionsMap ++ Map("dbtable" -> tableName)
    sparkSession.read.format("jdbc")
      .options(jdbcOptions)
      .load()

  }

  private def createSparkSession(broadcastThreshold: Int, defaultSizeInBytes: Long = Long.MaxValue): SparkSession = {
    SparkSession.builder()
      .appName("Spark broadcast join").master("local[*]")
      .config("spark.sql.autoBroadcastJoinThreshold", s"${broadcastThreshold}")
      .config("spark.sql.defaultSizeInBytes", s"${defaultSizeInBytes}")
      .config("spark.sql.join.preferSortMergeJoin", "false")
      .getOrCreate()
  }

}
