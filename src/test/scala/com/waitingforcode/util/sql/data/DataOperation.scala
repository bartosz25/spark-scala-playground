package com.waitingforcode.util.sql.data

import java.sql.PreparedStatement

abstract class DataOperation {

  def populatePreparedStatement(preparedStatement: PreparedStatement)

}
