package com.waitingforcode.util

import org.h2.tools.DeleteDbFiles
import java.sql.DriverManager

import com.waitingforcode.util.sql.data.DataOperation

object InMemoryDatabase {

  val DbName = "testdb"
  val DbDriver = "org.h2.Driver"
  val DbConnection = "jdbc:h2:~/" + DbName+";TRACE_LEVEL_FILE=3"
  val DbUser = "root"
  val DbPassword = ""

  lazy val connection = {
    Class.forName(DbDriver)
    val dbConnection = DriverManager.getConnection(DbConnection, DbUser, DbPassword)
    dbConnection.setAutoCommit(false)
    dbConnection
  }

  def createTable(query: String): InMemoryDatabase.type= {
    val createPreparedStatement = connection.prepareStatement(query)
    createPreparedStatement.executeUpdate()
    createPreparedStatement.close()
    connection.commit()
    this
  }

  def cleanDatabase() {
    DeleteDbFiles.execute("~", DbName, true)
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
