package com.waitingforcode.sql

import java.sql.PreparedStatement
import java.util.concurrent.ThreadLocalRandom

import com.waitingforcode.util.InMemoryDatabase
import com.waitingforcode.util.sql.data.DataOperation
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable

class LoadingDataTest extends FlatSpec with Matchers with BeforeAndAfter  {

  val sparkSession: SparkSession = SparkSession.builder()
      .appName("Spark SQL automatic schema resolution test").master("local[*]").getOrCreate()

  before {
    case class OrderDataOperation(id: Int, amount: Double) extends DataOperation {
      override def populatePreparedStatement(preparedStatement: PreparedStatement): Unit = {
        preparedStatement.setInt(1, id)
        preparedStatement.setDouble(2, amount)
      }
    }
    val ordersToInsert = mutable.ListBuffer[OrderDataOperation]()
    for (id <- 1 to 1000) {
      ordersToInsert.append(OrderDataOperation(id, ThreadLocalRandom.current().nextDouble(2000d)))
    }

    InMemoryDatabase.createTable("CREATE TABLE orders(id integer primary key, amount decimal(6,2))")
    InMemoryDatabase.populateTable("INSERT INTO orders (id, amount) VALUES (?, ?)", ordersToInsert)
  }

  after {
    sparkSession.stop()
    InMemoryDatabase.cleanDatabase()
  }

  "all orders" should "brought to the driver" in {
    val ordersReader = sparkSession.read.format("jdbc")
      .option("url", InMemoryDatabase.DbConnection)
      .option("driver", InMemoryDatabase.DbDriver)
      .option("dbtable", "orders")
      .option("user", InMemoryDatabase.DbUser)
      .option("password", InMemoryDatabase.DbPassword)
      .load()

    import sparkSession.implicits._
    val ordersIds: Array[Int] = ordersReader.select("id")
      .map(row => row.getInt(0))
      .collect()

    ordersIds.size shouldEqual(1000)
    for (id <- 1 to 1000) {
      ordersIds.contains(id) shouldBe true
    }
  }

}
