package com.waitingforcode.sql

import java.io.{File, PrintWriter}
import java.sql.PreparedStatement
import java.util.UUID

import com.waitingforcode.util.InMemoryDatabase
import com.waitingforcode.util.sql.data.DataOperation
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable
import scala.reflect.io.Directory

class PredicatePushdownTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession =
    SparkSession.builder().appName("Predicate pushdown test").master("local")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()

  private val Options =  Map("url" -> InMemoryDatabase.DbConnection, "dbtable" -> "orders", "user" -> InMemoryDatabase.DbUser,
    "password" -> InMemoryDatabase.DbPassword, "driver" -> InMemoryDatabase.DbDriver)

  private val ShopsParquetFile = new File("./structured_data.parquet")

  private val OutputDir = "/tmp/spark-predicate_pushdown-test"

  override def beforeAll(): Unit = {

    Directory(OutputDir).createDirectory(true, false)
    new PrintWriter(s"${OutputDir}/sample_order.json") { write("{\"id\": 1, \"amount\": 300}"); close }

    InMemoryDatabase.createTable("CREATE TABLE IF NOT EXISTS orders " +
      "(id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, shop_id INT(1) NOT NULL,  " +
      "customer VARCHAR(255) NOT NULL, amount DECIMAL(6, 2) NOT NULL)")

    case class Order(shopId: Int, customer: String, amount: Double) extends DataOperation {
      override def populatePreparedStatement(preparedStatement: PreparedStatement): Unit = {
        preparedStatement.setInt(1, shopId)
        preparedStatement.setString(2, customer)
        preparedStatement.setDouble(3, amount)
      }
    }

    val ordersToInsert = mutable.ListBuffer[Order]()
    ordersToInsert.append(Order(1, UUID.randomUUID().toString, 5))
    ordersToInsert.append(Order(2, UUID.randomUUID().toString, 15))
    ordersToInsert.append(Order(3, UUID.randomUUID().toString, 25))
    InMemoryDatabase.populateTable("INSERT INTO orders (shop_id, customer, amount) VALUES (?, ?, ?)",
      ordersToInsert)
  }

  override def afterAll(): Unit = {
    ShopsParquetFile.delete()
    InMemoryDatabase.cleanDatabase()
    Directory(OutputDir).deleteRecursively()

    sparkSession.stop()
  }

  "predicate pushdown" should "be used for relational database" in {
    val allOrders = sparkSession.read
      .format("jdbc")
      .options(Options)
      .load()

    val valuableOrdersQuery = allOrders
      .select("id", "amount")
      // using the BigDecimal is mandatory because the amount is a decimal
      // otherwise the generated filter will be composed of cast operation.
      // It makes that the filter won't be pushed down (cast is not "pushable")
      .where(allOrders("amount") > BigDecimal("10.0"))
    val valuableOrders = valuableOrdersQuery.collect()

    valuableOrders.size shouldEqual 2
    valuableOrdersQuery.queryExecution.toString() should include("== Physical Plan ==" +
      "\n*Scan JDBCRelation(orders) [numPartitions=1] [id#0,amount#3] PushedFilters: [*GreaterThan(AMOUNT,10.00)], " +
      "ReadSchema: struct<id:int,amount:decimal(6,2)>")
  }

  "predicate pushdown" should "not be applied when the columns are of different types" in {
    val allOrders = sparkSession.read
      .format("jdbc")
      .options(Options)
      .load()

    val valuableOrdersQuery = allOrders
      .select("id", "amount")
      .where(allOrders("amount") > 10)
    val valuableOrders = valuableOrdersQuery.collect()

    valuableOrders.size shouldEqual 2
    // Physical plan is:
    // == Physical Plan ==
    // *Filter (cast(AMOUNT#16 as decimal(12,2)) > 10.00)
    // +- *Scan JDBCRelation(orders) [numPartitions=1] [id#13,amount#16] ReadSchema: struct<id:int,amount:decimal(6,2)>
    valuableOrdersQuery.queryExecution.toString() should not include("PushedFilters: [*GreaterThan(AMOUNT,10.00)], " +
      "ReadSchema: struct<id:int,amount:decimal(6,2)>")
  }

  "predicate pushdown" should "be used when the filtering clause is used to make the join" in {
    import sparkSession.implicits._
    val shops = Seq(
      (1, "Shop_1"), (2, "Shop_2")
    ).toDF("id", "Name")
    val allOrders = sparkSession.read
      .format("jdbc")
      .options(Options)
      .load()

    val valuableOrdersQuery = allOrders.join(shops, allOrders("amount") > BigDecimal(10), "inner")

    // Please note that this query fails when the cross join is not
    // enabled explicitly through spark.sql.crossJoin.enabled
    // The physical plan is similar to (indexes after field names can change):
    // == Physical Plan ==
    // BroadcastNestedLoopJoin BuildRight, Inner
    // :- *Scan JDBCRelation(orders) [numPartitions=1] [ID#36,SHOP_ID#37,CUSTOMER#38,AMOUNT#39]
    // PushedFilters: [*GreaterThan(AMOUNT,10.00)], ReadSchema: struct<ID:int,SHOP_ID:int,CUSTOMER:string,AMOUNT:decimal(6,2)>
    //  +- BroadcastExchange IdentityBroadcastMode
    // +- LocalTableScan [id#31, Name#32]
    valuableOrdersQuery.queryExecution.toString() should include("== Physical Plan ==" +
      "\nBroadcastNestedLoopJoin BuildRight, Inner" +
      "\n:- *Scan JDBCRelation(orders) [numPartitions=1]")
    valuableOrdersQuery.queryExecution.toString() should include("PushedFilters: [*GreaterThan(AMOUNT,10.00)], " +
      "ReadSchema: struct<ID:int,SHOP_ID:int,CUSTOMER:string,AMOUNT:decimal(6,2)>" +
      "\n+- BroadcastExchange IdentityBroadcastMode")
  }

  "predicate pushdown" should "be applied on Parquet files" in {
    import sparkSession.implicits._
    val shops = Seq(
      (1, "Shop_1", 1000), (2, "Shop_2", 1100), (3, "Shop_3", 900)
    ).toDF("id", "name", "total_revenue")
    shops.write.mode(SaveMode.Overwrite).parquet(ShopsParquetFile.getAbsolutePath)

    val shopsFromDisk = sparkSession.read
      .format("parquet")
      .parquet(ShopsParquetFile.getAbsolutePath)

    val valuableShopsQuery = shopsFromDisk.where("total_revenue > 1000")

    valuableShopsQuery.collect().size shouldEqual 1
    // Expected physical plan is:
    // == Physical Plan ==
    // *Project [id#93, name#94, total_revenue#95]
    // +- *Filter (isnotnull(total_revenue#95) && (total_revenue#95 > 1000))
    // +- *FileScan parquet [id#93,name#94,total_revenue#95] Batched: true, Format: Parquet,
    // Location: InMemoryFileIndex[file:/home/bartosz/workspace/spark-scala/structured_data.parquet],
    // PartitionFilters: [], PushedFilters: [IsNotNull(total_revenue), GreaterThan(total_revenue,1000)],
    // ReadSchema: struct<id:int,name:string,total_revenue:int>
    valuableShopsQuery.queryExecution.toString() should include("PartitionFilters: [], " +
      "PushedFilters: [IsNotNull(total_revenue), GreaterThan(total_revenue,1000)], " +
      "ReadSchema: struct<id:int,name:string,total_revenue:int>")
  }

  "predicate pushdown" should "not be applied on JSON files" in {
    val allOrders = sparkSession.read
      .format("json")
      .load(OutputDir)

    val valuableOrdersQuery = allOrders.where("amount > 10")
    val valuableOrders = valuableOrdersQuery.count

    valuableOrders shouldEqual 1
    println(s"plan=${valuableOrdersQuery.queryExecution.toString().trim }")
    // Expected plan is:
    // == Physical Plan ==
    //  *Project [amount#8L, id#9L]
    // +- *Filter (isnotnull(amount#8L) && (amount#8L > 10))
    // +- *FileScan json [amount#8L,id#9L] Batched: false, Format: JSON,
    // Location: InMemoryFileIndex[file:/tmp/spark-predicate_pushdown-test], PartitionFilters: [],
    // PushedFilters: [IsNotNull(amount), GreaterThan(amount,10)], ReadSchema: struct<amount:bigint,id:bigint>
    // As you can see, it also has the pushed filters. But if you look at JsonFileFormat source you'll see
    // that these filter are never used.
    valuableOrdersQuery.queryExecution.toString() should include("PushedFilters: [IsNotNull(amount), GreaterThan(amount,10)],")
    valuableOrdersQuery.queryExecution.toString() should include(" +- *FileScan json [amount")
  }
}
