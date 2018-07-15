package com.waitingforcode.sql

import java.sql.{PreparedStatement, ResultSet}
import java.util.Properties

import com.waitingforcode.util.MysqlConnector
import com.waitingforcode.util.sql.data.DataOperation
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}


class RdbmsSaveModesTest extends FlatSpec  with BeforeAndAfter with Matchers {

  // Test on MySQL because H2 is NoopDialect and it returns None for
  // org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.isCascadingTruncateTable
  // and because of that every time the table is recreated
  private val Connection = "jdbc:mysql://127.0.0.1:3306/wfc_tests?serverTimezone=UTC"
  private val User = "root"
  private val Password = "root"
  private val mysqlConnector = new MysqlConnector(Connection, User, Password)
  before {
    mysqlConnector.executedSideEffectQuery("CREATE TABLE users (user_login VARCHAR(20) NOT NULL)")
    val users = (1 to 5).map(id => UserInformation(s"User${id}"))
    mysqlConnector.populateTable("INSERT INTO users (user_login) VALUES (?)", users)
  }

  after {
    mysqlConnector.executedSideEffectQuery("DROP TABLE users")
  }

  private val sparkSession: SparkSession = SparkSession.builder().appName("Spark SQL RDBMS save modes")
    .master("local[*]").getOrCreate()
  import sparkSession.implicits._

  private val ConnectionProperties = new Properties()
  ConnectionProperties.setProperty("user", User)
  ConnectionProperties.setProperty("password", Password)

  private def mapResultSetToColumnType: (ResultSet) => String = (resultSet) => {
    // index#1 = column name
    // index#2 = column type
    resultSet.getString(2)
  }

  "Spark" should "override the column type with overwrite save mode" in {
    val newUsers = Seq(
      ("User100"), ("User101"), ("User102")
    ).toDF("user_login")

    newUsers.write.mode(SaveMode.Overwrite).jdbc(Connection, "users", ConnectionProperties)

    val allUsers = mysqlConnector.getRows("SELECT * FROM users", (resultSet) => resultSet.getString("user_login"))
    allUsers should have size 3
    allUsers should contain allOf("User100", "User101", "User102")
    val columnTypes = mysqlConnector.getRows("DESC users", mapResultSetToColumnType)
    columnTypes should have size 1
    columnTypes(0) shouldEqual "text"
  }

  "truncate option enabled" should "prevent table against recreating with unexpected type" in {
    val newUsers = Seq(
      ("User100"), ("User101"), ("User102")
    ).toDF("user_login")

    newUsers.write.mode(SaveMode.Overwrite)
      .option("truncate", true)
      .jdbc(Connection, "users", ConnectionProperties)

    val allUsers = mysqlConnector.getRows("SELECT * FROM users", (resultSet) => resultSet.getString("user_login"))
    allUsers should have size 3
    allUsers should contain allOf("User100", "User101", "User102")
    val columnTypes = mysqlConnector.getRows("DESC users", mapResultSetToColumnType)
    columnTypes should have size 1
    columnTypes(0) shouldEqual "varchar(20)"
  }

  "append mode" should "add new rows without removing existing data or deleting the table" in {
    val newUsers = Seq(
      ("User100"), ("User101"), ("User102")
    ).toDF("user_login")

    newUsers.write.mode(SaveMode.Append)
      .jdbc(Connection, "users", ConnectionProperties)

    val allUsers = mysqlConnector.getRows("SELECT * FROM users", (resultSet) => resultSet.getString("user_login"))
    allUsers should have size 8
    allUsers should contain allOf("User1", "User2", "User3", "User4", "User5",  "User100", "User101", "User102")
    val columnTypes = mysqlConnector.getRows("DESC users", mapResultSetToColumnType)
    columnTypes should have size 1
    columnTypes(0) shouldEqual "varchar(20)"
  }

  "ignore mode" should "do nothing when the table already exists" in {
    val newUsers = Seq(
      ("User100"), ("User101"), ("User102")
    ).toDF("user_login")

    newUsers.write.mode(SaveMode.Ignore)
      .jdbc(Connection, "users", ConnectionProperties)

    val allUsers = mysqlConnector.getRows("SELECT * FROM users", (resultSet) => resultSet.getString("user_login"))
    allUsers should have size 5
    allUsers should contain allOf("User1", "User2", "User3", "User4", "User5")
    val columnTypes = mysqlConnector.getRows("show columns from users", mapResultSetToColumnType)
    columnTypes should have size 1
    columnTypes(0) shouldEqual "varchar(20)"
  }

  "error mode" should "throw an exception for insert to already existing table" in {
    import sparkSession.implicits._
    val newUsers = Seq(
      ("User100"), ("User101"), ("User102")
    ).toDF("user_login")

    val analysisException = intercept[AnalysisException] {
      newUsers.write.mode(SaveMode.ErrorIfExists)
        .jdbc(Connection, "users", ConnectionProperties)
    }
    analysisException.message should include("Table or view 'users' already exists.")
  }

}

case class UserInformation(login: String) extends DataOperation {
  override def populatePreparedStatement(preparedStatement: PreparedStatement): Unit = {
    preparedStatement.setString(1, login)
  }
}

