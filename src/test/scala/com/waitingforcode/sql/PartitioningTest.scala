package com.waitingforcode.sql

import java.math.BigDecimal
import java.sql.PreparedStatement
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import com.waitingforcode.util.InMemoryDatabase
import com.waitingforcode.util.sql.data.DataOperation
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable

class PartitioningTest extends FlatSpec with BeforeAndAfter with Matchers {

  var sparkSession: SparkSession = null

  before {
    sparkSession = SparkSession.builder().appName("Partitioning test").master("local").getOrCreate()
    InMemoryDatabase.createTable("CREATE TABLE IF NOT EXISTS orders_sql_partitioning " +
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
    for (shopId <- 0 until 9) {
      for (i <- 1 to 50) {
        val amount = ThreadLocalRandom.current().nextDouble(1000)
        ordersToInsert.append(Order(shopId, UUID.randomUUID().toString, amount))
      }
    }
    InMemoryDatabase.populateTable("INSERT INTO orders_sql_partitioning (shop_id, customer, amount) VALUES (?, ?, ?)",
      ordersToInsert)
  }

  after {
    sparkSession.stop()
    InMemoryDatabase.cleanDatabase()
  }

  "the number of partitions" should "be the same as the number of distinct shop_ids" in {
    val lowerBound = 0
    val upperBound = 9
    val numberOfPartitions = 9
    val jdbcDF = sparkSession.read
      .format("jdbc")
      .options(getOptionsMap(numberOfPartitions, lowerBound, upperBound))
      .load()

    jdbcDF.select("shop_id")
      .foreachPartition((partitionRows: Iterator[Row]) => {
        val shops = partitionRows.map(row => row.getAs[Int]("shop_id")).toSet
        DataPerPartitionHolder.PartitionsEqualToShopIds.append(shops)
      })

    DataPerPartitionHolder.PartitionsEqualToShopIds.size shouldEqual(9)
    DataPerPartitionHolder.PartitionsEqualToShopIds(0) should contain only(0)
    DataPerPartitionHolder.PartitionsEqualToShopIds(1) should contain only(1)
    DataPerPartitionHolder.PartitionsEqualToShopIds(2) should contain only(2)
    DataPerPartitionHolder.PartitionsEqualToShopIds(3) should contain only(3)
    DataPerPartitionHolder.PartitionsEqualToShopIds(4) should contain only(4)
    DataPerPartitionHolder.PartitionsEqualToShopIds(5) should contain only(5)
    DataPerPartitionHolder.PartitionsEqualToShopIds(6) should contain only(6)
    DataPerPartitionHolder.PartitionsEqualToShopIds(7) should contain only(7)
    DataPerPartitionHolder.PartitionsEqualToShopIds(8) should contain only(8)
  }

  // Partitioning logic:
  // If ($upperBound - $lowerBound >= $nbPartitions) => keep the $nbPartitions
  // Otherwise reduce the number of partitions to $upperBound - $lowerBound
  "the number of partitions" should "be reduced when the difference between upper and lower bounds are lower than the " +
    "number of expected partitions" in {
    val lowerBound = 0
    val upperBound = 2
    val numberOfPartitions = 5

    val jdbcDF = sparkSession.read
      .format("jdbc")
      .options(getOptionsMap(numberOfPartitions, lowerBound, upperBound))
      .load()

    jdbcDF.select("shop_id")
      .foreachPartition((partitionRows: Iterator[Row]) => {
        val shops = partitionRows.map(row => row.getAs[Int]("shop_id")).toSet
        DataPerPartitionHolder.DataPerPartitionReducedPartitionsNumber.append(shops)
      })

    DataPerPartitionHolder.DataPerPartitionReducedPartitionsNumber.size shouldEqual(2)
    DataPerPartitionHolder.DataPerPartitionReducedPartitionsNumber(0) should contain only(0)
    DataPerPartitionHolder.DataPerPartitionReducedPartitionsNumber(1) should contain allOf(1, 2, 3, 4, 5, 6, 7, 8)
  }

  "partitions" should "be divided according to the stride equal to 1" in {
    val lowerBound = 0
    val upperBound = 8
    val numberOfPartitions = 5
    // Stride = (8/5) - (0/3) ~ 0.533 ~ 1
    // We expect 8 partitions and according to the partitioning algorithm, Spark SQL will generate
    // the queries with following boundaries:
    // 1) shop_id < 1 OR shop_id IS NULL
    // 2) shop_id >= 1 AND shop_id < 2
    // 3) shop_id >= 2 AND shop_id < 3
    // 4) shop_id >= 3 AND shop_id < 4
    // 5) shop_id >= 4

    val jdbcDF = sparkSession.read
      .format("jdbc")
      .options(getOptionsMap(numberOfPartitions, lowerBound, upperBound))
      .load()

    jdbcDF.select("shop_id")
      .foreachPartition((partitionRows: Iterator[Row]) => {
        val shops = partitionRows.map(row => row.getAs[Int]("shop_id")).toSet
        DataPerPartitionHolder.DataPerPartitionStridesComputationExample.append(shops)
      })

    DataPerPartitionHolder.DataPerPartitionStridesComputationExample.size shouldEqual(5)
    DataPerPartitionHolder.DataPerPartitionStridesComputationExample(0) should contain only(0)
    DataPerPartitionHolder.DataPerPartitionStridesComputationExample(1) should contain only(1)
    DataPerPartitionHolder.DataPerPartitionStridesComputationExample(2) should contain only(2)
    DataPerPartitionHolder.DataPerPartitionStridesComputationExample(3) should contain only(3)
    DataPerPartitionHolder.DataPerPartitionStridesComputationExample(4) should contain allOf(4, 5, 6, 7, 8)
  }

  "two empty partitions" should "be created when the upper bound is too big" in {
    val lowerBound = 0
    // Here upperBound is much bigger than the maximum shop_id value (8)
    // In consequence, empty partitions will be generated
    val upperBound = 20
    val numberOfPartitions = 5
    //
    // Stride = (20/5) - (0/5) = 4
    // We expect 8 partitions and according to the partitioning algorithm, Spark SQL will generate
    // the queries with following boundaries:
    // 1) shop_id < 4 OR shop_id IS NULL
    // 2) shop_id >= 4 AND shop_id < 8
    // 3) shop_id >= 8 AND shop_id < 12
    // 4) shop_id >= 12 AND shop_id < 16
    // 5) shop_id >= 16

    val jdbcDF = sparkSession.read
      .format("jdbc")
      .options(getOptionsMap(numberOfPartitions, lowerBound, upperBound))
      .load()


    jdbcDF.select("shop_id").foreachPartition((partitionRows: Iterator[Row]) => {
      val shops = partitionRows.map(row => row.getAs[Int]("shop_id")).toSet
      DataPerPartitionHolder.DataPerPartitionEmptyPartitions.append(shops)
    })

    DataPerPartitionHolder.DataPerPartitionEmptyPartitions.size shouldEqual(5)
    DataPerPartitionHolder.DataPerPartitionEmptyPartitions(0) should contain allOf(0, 1, 2, 3)
    DataPerPartitionHolder.DataPerPartitionEmptyPartitions(1) should contain allOf(4, 5, 6, 7)
    DataPerPartitionHolder.DataPerPartitionEmptyPartitions(2) should contain only(8)
    DataPerPartitionHolder.DataPerPartitionEmptyPartitions(3) shouldBe  empty
    DataPerPartitionHolder.DataPerPartitionEmptyPartitions(4) shouldBe  empty
  }

  "the decimal partition column" should "be accepted" in {
    val lowerBound = 0
    val upperBound = 1000
    val numberOfPartitions = 2

    val jdbcDF = sparkSession.read
      .format("jdbc")
      .options(getOptionsMap(numberOfPartitions, lowerBound, upperBound, "amount"))
      .load()

    jdbcDF.select("amount")
      .foreachPartition((partitionRows: Iterator[Row]) => {
        val shops = partitionRows.map(row => row.getAs[BigDecimal]("amount")).toSet
        DataPerPartitionHolder.DataPerPartitionDecimalColumn.append(shops)
        println(s">>> ${shops.mkString(",")}")
      })

    DataPerPartitionHolder.DataPerPartitionDecimalColumn.size shouldEqual(2)
    // Do not make exact assertions since the amount is generated randomly
    // and the test can fail from time to time
    DataPerPartitionHolder.DataPerPartitionDecimalColumn(0) should not be (empty)
    DataPerPartitionHolder.DataPerPartitionDecimalColumn(1) should not be (empty)
  }

  private def getOptionsMap(numberOfPartitions: Int, lowerBound: Int, upperBound: Int,
                            column: String = "shop_id"): Map[String, String] = {
    Map("url" -> InMemoryDatabase.DbConnection, "dbtable" -> "orders_sql_partitioning", "user" -> InMemoryDatabase.DbUser,
      "password" -> InMemoryDatabase.DbPassword, "driver" -> InMemoryDatabase.DbDriver,
      "partitionColumn" -> s"${column}", "numPartitions" -> s"${numberOfPartitions}", "lowerBound" -> s"${lowerBound}",
      "upperBound" -> s"${upperBound}")
  }
}

object DataPerPartitionHolder {

  val PartitionsEqualToShopIds = mutable.ListBuffer[Set[Int]]()

  val DataPerPartitionReducedPartitionsNumber = mutable.ListBuffer[Set[Int]]()

  val DataPerPartitionStridesComputationExample = mutable.ListBuffer[Set[Int]]()

  val DataPerPartitionEmptyPartitions = mutable.ListBuffer[Set[Int]]()

  val DataPerPartitionDecimalColumn = mutable.ListBuffer[Set[BigDecimal]]()

}
