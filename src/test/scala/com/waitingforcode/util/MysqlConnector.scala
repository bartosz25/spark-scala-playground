package com.waitingforcode.util

import java.sql.DriverManager

import com.waitingforcode.util.sql.data.DataOperation

class MysqlConnector(url: String, user: String, password: String) {

  lazy val connection = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val dbConnection = DriverManager.getConnection(url, user, password)
    dbConnection.setAutoCommit(false)
    dbConnection
  }

  def cleanTable(tableName: String): Unit = {
    val statement = connection.prepareStatement(s"TRUNCATE TABLE ${tableName}")
    statement.execute()
    statement.close()
    connection.commit()
  }

  def populateTable[T <: DataOperation](populateQuery: String, dataToInsert: Seq[T]) = {
    for (data <- dataToInsert) {
      val preparedStatement = connection.prepareStatement(populateQuery)
      data.populatePreparedStatement(preparedStatement)
      preparedStatement.execute()
      preparedStatement.close()
    }
    connection.commit()
  }

}
