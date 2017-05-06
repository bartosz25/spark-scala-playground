package com.waitingforcode.util.sql.data
import java.sql.PreparedStatement

case class CityDataOperation(name: String, country: String) extends DataOperation {
  override def populatePreparedStatement(preparedStatement: PreparedStatement): Unit = {
    preparedStatement.setString(1, name)
    preparedStatement.setString(2, country)
  }
}
