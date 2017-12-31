package com.waitingforcode.sql

import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.reflect.io.File

class SaveModesTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder().appName("SaveMode test").master("local")
    .getOrCreate()

  private val BaseDir = "/tmp/spark-save-modes/"

  private val FileToAppend = s"${BaseDir}appendable_file"

  private val FileToOverwrite = s"${BaseDir}overridable_file"

  private val AlreadyExistentFile = s"${BaseDir}already_existent_file"

  private val AlreadyExistentOrder = Order(1, 1, 19d)

  import sparkSession.implicits._

  override def beforeAll() = {
    val ordersDataFrame = Seq(
      (1, 1, 19d)
    ).toDF("id", "customers_id", "amount")

    ordersDataFrame.write.json(FileToAppend)
    ordersDataFrame.write.json(FileToOverwrite)
    ordersDataFrame.write.json(AlreadyExistentFile)
  }

  override def afterAll() {
    File(FileToAppend).deleteRecursively()
    File(FileToOverwrite).deleteRecursively()
    File(AlreadyExistentFile).deleteRecursively()
    sparkSession.stop()
  }

  "created data" should "be appended to already existent data source" in {
    val newOrders = Seq(
      (10, 100, 2000d), (11, 100, 2500d)
    ).toDF("id", "customers_id", "amount")

    newOrders.write.mode(SaveMode.Append).json(FileToAppend)

    val allOrders = sparkSession.read.json(FileToAppend).map(Converter.rowToOrder(_)).collect()

    allOrders should have length(3)
    allOrders should contain allOf(AlreadyExistentOrder, Order(10, 100,  2000d), Order(11, 100, 2500d))
  }

  "created data" should "overwrite already existent data source" in {
    val newOrders = Seq(
      (20, 200, 3000d), (21, 200, 3500d)
    ).toDF("id", "customers_id", "amount")

    newOrders.write.mode(SaveMode.Overwrite).json(FileToOverwrite)

    val allOrders = sparkSession.read.json(FileToOverwrite).map(Converter.rowToOrder(_)).collect()

    allOrders should have length(2)
    allOrders should contain allOf(Order(20, 200,  3000d), Order(21, 200, 3500d))
  }

  "already existent data source" should "produce an error if the DataFrame is saved to the same location" in {
    val newOrders = Seq(
      (30, 300, 4000d), (31, 300, 4500d)
    ).toDF("id", "customers_id", "amount")

    val writingError = intercept[AnalysisException] {
      newOrders.write.mode(SaveMode.ErrorIfExists).json(AlreadyExistentFile)
    }

    writingError.getMessage should include ("path file:/tmp/spark-save-modes/already_existent_file already exists")
  }

  "created data" should "not be saved when the data source already exists" in {
    val newOrders = Seq(
      (30, 300, 4000d), (31, 300, 4500d)
    ).toDF("id", "customers_id", "amount")

    newOrders.write.mode(SaveMode.Ignore).json(AlreadyExistentFile)

    val allOrders = sparkSession.read.json(AlreadyExistentFile).map(Converter.rowToOrder(_)).collect()

    allOrders should have length(1)
    allOrders(0) should equal(AlreadyExistentOrder)
  }

}

object Converter {

  def rowToOrder(orderRow: Row): Order = {
    Order(orderRow.getAs[Long]("id"), orderRow.getAs[Long]("customers_id"), orderRow.getAs[Double]("amount"))
  }
}

case class Order(id: Long, customer: Long, amount: Double)
