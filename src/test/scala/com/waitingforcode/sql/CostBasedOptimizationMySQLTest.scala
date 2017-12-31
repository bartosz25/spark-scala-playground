package com.waitingforcode.sql

import java.sql.PreparedStatement
import java.util.UUID

import com.waitingforcode.util.MysqlConnector
import com.waitingforcode.util.sql.data.DataOperation
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable

class CostBasedOptimizationMySQLTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val Connection = "jdbc:mysql://127.0.0.1:3306/wfc_tests?serverTimezone=UTC&useCursorFetch=true&autocommit=false"
  private val User = "root"
  private val Password = "root"
  private val mysqlConnector = new MysqlConnector(Connection, User, Password)

  private val sparkSession: SparkSession = SparkSession.builder().appName("Cost-Based Optimizer test").master("local")
    .config("spark.sql.cbo.enabled", "true")
    .config("spark.sql.cbo.joinReorder.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()
  import sparkSession.implicits._

  override def beforeAll(): Unit = {
    case class City(country: String, name: String) extends DataOperation {
      override def populatePreparedStatement(preparedStatement: PreparedStatement): Unit = {
        preparedStatement.setString(1, country)
        preparedStatement.setString(2, name)
      }
    }
    case class Country(code: String, name: String) extends DataOperation {
      override def populatePreparedStatement(preparedStatement: PreparedStatement): Unit = {
        preparedStatement.setString(1, code)
        preparedStatement.setString(2, name)
      }
    }
    val countriesToInsert = mutable.ListBuffer[Country]()
    val citiesToInsert = mutable.ListBuffer[City]()
    for (i <- 1 to 10) {
      val countryCode = UUID.randomUUID().toString
      countriesToInsert.append(Country(countryCode, s"Country${i}"))
      for (j <- 1 to 15) {
        citiesToInsert.append(City(countryCode, s"City_${i}_${j}"))
      }
    }
    mysqlConnector.populateTable("INSERT INTO countries (code, name) VALUES (?, ?)", countriesToInsert)
    mysqlConnector.populateTable("INSERT INTO cities (country, name) VALUES (?, ?)", citiesToInsert)
  }

  override def afterAll() {
    mysqlConnector.cleanTable("countries")
    mysqlConnector.cleanTable("cities")
    sparkSession.stop()
  }

  "CBO" should "not be used for MySQL without ANALYZED command" in {
    executeJoinQuery(false).explain(true)
  }

  "CBO" should "not be used for MySQL even with ANALYZED command executed" in {
    executeJoinQuery(true).explain(true)
  }

  def executeJoinQuery(analyzeTables: Boolean): DataFrame = {
    val countriesDataFrame = sparkSession.read
      .format("jdbc")
      .options(getOptionsMap("countries"))
      .load()
    val citiesDataFrame = sparkSession.read
      .format("jdbc")
      .options(getOptionsMap("cities"))
      .load()
    if (analyzeTables) {
      sparkSession.sql(s"ANALYZE TABLE countries COMPUTE STATISTICS")
      sparkSession.sql(s"ANALYZE TABLE cities COMPUTE STATISTICS")
    }

    countriesDataFrame.select("code", "name")
      .filter("name != 'AAA'")
      .join(citiesDataFrame, $"country" === $"code")
  }

  private def getOptionsMap(tableName: String): Map[String, String] = {
    Map("url" -> s"${Connection}",
      "dbtable" -> s"${tableName}", "user" -> s"${User}", "password" -> s"${Password}",
      "driver" -> "com.mysql.cj.jdbc.Driver")
  }
}
