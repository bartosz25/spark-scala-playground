package com.waitingforcode.sql

import java.sql.PreparedStatement
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import com.waitingforcode.util.MysqlConnector
import com.waitingforcode.util.javaassist.{MethodInvocationCounter, MethodInvocationDecorator}
import com.waitingforcode.util.sql.data.DataOperation
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable

/**
  * The test shows the behavior of fetch size. It's executed
  * against a MySQL database. Before launching it, create the table with:
  * CREATE TABLE orders (
  *   id INT(11) NOT NULL AUTO_INCREMENT,
  *   customer VARCHAR(36) NOT NULL,
  *   amount DECIMAL(6, 2) NOT NULL,
  *   PRIMARY KEY(id)
  * )
  */
class FetchsizeTest extends FlatSpec with Matchers with BeforeAndAfter {

  private val FetchMoreRowsKey = "fetchMoreRows"
  MethodInvocationDecorator.decorateClass("com.mysql.cj.mysqla.result.ResultsetRowsCursor", FetchMoreRowsKey).toClass

  private val Connection = "jdbc:mysql://127.0.0.1:3306/wfc_tests?serverTimezone=UTC&useCursorFetch=true&autocommit=false"
  private val User = "root"
  private val Password = "root"
  private val mysqlConnector = new MysqlConnector(Connection, User, Password)

  private var sparkSession: SparkSession = null

  before {
    sparkSession = SparkSession.builder().appName("Spark SQL fetch size test").master("local[*]").getOrCreate()
    MethodInvocationCounter.methodInvocations.remove(FetchMoreRowsKey)
    case class Order(customer: String, amount: Double) extends DataOperation {
      override def populatePreparedStatement(preparedStatement: PreparedStatement): Unit = {
        preparedStatement.setString(1, customer)
        preparedStatement.setDouble(2, amount)
      }
    }
    val ordersToInsert = mutable.ListBuffer[Order]()
    for (i <- 1 to 1000) {
      val amount = ThreadLocalRandom.current().nextDouble(1000)
      ordersToInsert.append(Order(UUID.randomUUID().toString, amount))
    }
    mysqlConnector.populateTable("INSERT INTO orders (customer, amount) VALUES (?, ?)", ordersToInsert)
  }

  after {
    mysqlConnector.cleanTable("orders")
    sparkSession.stop()
  }

  "fetch size smaller than the number of rows" should "make more than 1 round trip" in {
    val jdbcDataFrame = sparkSession.read
      .format("jdbc")
      .options(getOptionsMap(10))
      .load()

    jdbcDataFrame.foreach(row => {})

    MethodInvocationCounter.methodInvocations("fetchMoreRows") shouldEqual(101)
  }

  "fetch size smaller than the number of rows with internal filter" should "make more less than 101 round trip" in {
    // As you can see, this test has the same parameters as the previous one
    // But in the action, instead of iterating over all rows, it ignores
    // all rows with id smaller than 20. Thus logically, it should make
    // only 2 round trips to get rows 1-10 and 11-20
    // Please note that this sample works also because we use a single
    // partition. Otherwise, it should call rows.next() at least
    // once for each of partitions
    val jdbcDataFrame = sparkSession.read
      .format("jdbc")
      .options(getOptionsMap(10))
      .load()

    jdbcDataFrame.foreachPartition((rows: Iterator[Row]) => {
      while (rows.hasNext && rows.next.getAs[Int]("id") < 20) {
        // Do nothing, only to show the case
      }
    })

    MethodInvocationCounter.methodInvocations("fetchMoreRows") shouldEqual(2)
  }


  "fetch size greater than the number of rows" should "make only 1 round trip" in {
    val jdbcDataFrame = sparkSession.read
      .format("jdbc")
      .options(getOptionsMap(50000))
      .load()

    jdbcDataFrame.foreach(row => {})

    MethodInvocationCounter.methodInvocations("fetchMoreRows") shouldEqual(2)
  }

  private def getOptionsMap(fetchSize: Int): Map[String, String] = {
    Map("url" -> s"${Connection}",
      "dbtable" -> "orders", "user" -> s"${User}", "password" -> s"${Password}",
      "driver" -> "com.mysql.cj.jdbc.Driver", "fetchsize" -> s"${fetchSize}")
  }

}
