package com.waitingforcode.sql

import java.sql.PreparedStatement
import java.util.concurrent.ThreadLocalRandom

import com.waitingforcode.util.InMemoryDatabase
import com.waitingforcode.util.sql.data.DataOperation

import scala.collection.mutable

object JoinHelper {

  case class CustomerDataOperation(id: Int, login: String) extends DataOperation {
    override def populatePreparedStatement(preparedStatement: PreparedStatement): Unit = {
      preparedStatement.setInt(1, id)
      preparedStatement.setString(2, login)
    }
  }

  case class OrderDataOperation(customerId: Int, amount: Double) extends DataOperation {
    override def populatePreparedStatement(preparedStatement: PreparedStatement): Unit = {
      preparedStatement.setInt(1, customerId)
      preparedStatement.setDouble(2, amount)
    }
  }

  def createTables(): Unit = {
    InMemoryDatabase.createTable("CREATE TABLE customers(id integer primary key, login text)")
    InMemoryDatabase.createTable("CREATE TABLE orders(id integer auto_increment primary key, customers_id integer, amount decimal(6,2))")
  }

  def insertOrders(customerIds: Seq[Int], ordersNumber: Int) = {
    for (customer <- customerIds) {
      val ordersToInsert = mutable.ListBuffer[OrderDataOperation]()
      for (id <- 1 to ordersNumber) {
        ordersToInsert.append(OrderDataOperation(customer, ThreadLocalRandom.current().nextDouble(2000d)))
      }
      InMemoryDatabase.populateTable("INSERT INTO orders (customers_id, amount) VALUES (?, ?)", ordersToInsert)
    }
  }

  def insertCustomers(number: Int): Seq[Int] = {
    val customerIds = (1 to number).map(id => (id, s"User#${id}"))

    val customersToInsert = mutable.ListBuffer[CustomerDataOperation]()
    for (customer <- customerIds) {
      customersToInsert.append(CustomerDataOperation(customer._1, customer._2))
    }

    InMemoryDatabase.populateTable("INSERT INTO customers (id, login) VALUES (?, ?)", customersToInsert)

    customerIds.map(_._1)
  }

}
