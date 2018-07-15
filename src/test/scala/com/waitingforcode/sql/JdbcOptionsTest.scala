package com.waitingforcode.sql

import java.sql.PreparedStatement
import java.util.Properties

import com.waitingforcode.util.MysqlConnector
import com.waitingforcode.util.javaassist.{MethodInvocationCounter, MethodInvocationDecorator}
import com.waitingforcode.util.sql.data.DataOperation
import org.apache.spark.SparkException
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

class JdbcOptionsTest extends FlatSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  private val Connection = "jdbc:mysql://127.0.0.1:3306/spark_jdbc_options_test?serverTimezone=UTC"
  private val DbUser = "root"
  private val DbPassword = "root"
  private val ConnectionProperties = new Properties()
  ConnectionProperties.setProperty("user", DbUser)
  ConnectionProperties.setProperty("password", DbPassword)
  private val MysqlConnector = new MysqlConnector(Connection, DbUser, DbPassword)

  private val TransactionIsolationNoneTable = "transaction_isolation_test"
  private val TransactionIsolationReadCommittedTable = "transaction_isolation_read_committed_test"
  private val CreateTableOptionsTable = "create_table_options_test"
  private val CreateTableColumnTypesTable = "create_table_column_types_test"
  private val CustomSchemaTable = "custom_schema_test"
  private val BatchSizeTable = "batch_size_test"
  private val SessionInitTable = "session_init_test"

  override def afterAll(): Unit = {
    MysqlConnector.close()
  }

  private val ExecuteBatchMethodName = "executeBatch"
  before {
    MethodInvocationCounter.methodInvocations.remove(ExecuteBatchMethodName)
  }
  after {
    MysqlConnector.executedSideEffectQuery(s"DROP TABLE IF EXISTS ${TransactionIsolationNoneTable}")
    MysqlConnector.executedSideEffectQuery(s"DROP TABLE IF EXISTS ${TransactionIsolationReadCommittedTable}")
    MysqlConnector.executedSideEffectQuery(s"DROP TABLE IF EXISTS ${CreateTableOptionsTable}")
    MysqlConnector.executedSideEffectQuery(s"DROP TABLE IF EXISTS ${CreateTableColumnTypesTable}")
    MysqlConnector.executedSideEffectQuery(s"DROP TABLE IF EXISTS ${CustomSchemaTable}")
    MysqlConnector.executedSideEffectQuery(s"DROP TABLE IF EXISTS ${BatchSizeTable}")
    MysqlConnector.executedSideEffectQuery(s"DROP TABLE IF EXISTS ${SessionInitTable}")
  }

  private val sparkSession: SparkSession =  SparkSession.builder().appName("Spark SQL JDBC options")
    .master("local[*]").getOrCreate()
  import sparkSession.implicits._

  "None transaction level" should "insert rows without the transaction" in {
    MysqlConnector.executedSideEffectQuery(s"CREATE TABLE IF NOT EXISTS ${TransactionIsolationNoneTable} " +
      s"(id INT(11) NOT NULL PRIMARY KEY, login TEXT)")
    val usersToInsert = Seq(User(4, s"User#4"))
    MysqlConnector.populateTable(s"INSERT INTO ${TransactionIsolationNoneTable} (id, login) VALUES (?, ?)", usersToInsert)
    val users = (1 to 10).map(nr => User(nr, s"User#${nr}")).toDF("id", "login")
      .repartition(1)
      .orderBy($"id".asc)

    val insertException = intercept[SparkException] {
      users.write
        .mode(SaveMode.Append)
        .option("isolationLevel", "NONE")
        .option("batchsize", 1)
        .jdbc(Connection, TransactionIsolationNoneTable, ConnectionProperties)
    }

    insertException.getMessage should include("Duplicate entry '4' for key 'PRIMARY'")
    val MysqlConnector2 = new MysqlConnector(Connection, DbUser, DbPassword)
    val rows = MysqlConnector2.getRows(s"SELECT * FROM ${TransactionIsolationNoneTable}", (resultSet) => {
      (resultSet.getInt("id"), resultSet.getString("login"))
    })
    rows should have size 4
    rows should contain allOf((1, "User#1"), (2, "User#2"), (3, "User#3"), (4, "User#4"))
  }

  "READ_COMMITTED isolation level" should "insert all or none rows within a transaction" in {
    // Here we test exactly the same code as above but with different transaction level
    MysqlConnector.executedSideEffectQuery(s"CREATE TABLE IF NOT EXISTS ${TransactionIsolationReadCommittedTable} " +
      s"(id INT(11) NOT NULL PRIMARY KEY, login TEXT)")
    val usersToInsert = Seq(User(4, s"User#4"))
    MysqlConnector.populateTable(s"INSERT INTO ${TransactionIsolationReadCommittedTable} (id, login) VALUES (?, ?)", usersToInsert)

    val users = (1 to 10).map(nr => User(nr, s"User#${nr}")).toDF("id", "login")
      .orderBy($"id".asc)
      .repartition(1)

    val insertException = intercept[SparkException] {
      users.write
        .mode(SaveMode.Append)
        .option("isolationLevel", "READ_COMMITTED")
        .option("batchsize", 1)
        .jdbc(Connection, TransactionIsolationReadCommittedTable, ConnectionProperties)
    }

    insertException.getMessage should include("Duplicate entry '4' for key 'PRIMARY'")
    val rows = MysqlConnector.getRows(s"SELECT * FROM ${TransactionIsolationReadCommittedTable}", (resultSet) => {
      (resultSet.getInt("id"), resultSet.getString("login"))
    })
    rows should have size 1
    rows(0) shouldEqual (4, "User#4")
  }

  "createTableOptions " should "create table with custom ENGINE and CHARSET options" in {
    val users = (1 to 10).map(nr => User(nr, s"User#${nr}")).toDF("id", "login")

    // createTableOptions is appended to the end of the creation query:
    //val sql = s"CREATE TABLE $table ($strSchema) $createTableOptions"
    users.write
      .mode(SaveMode.Overwrite)
      .option("createTableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Spark SQL test table'")
      .jdbc(Connection, CreateTableOptionsTable, ConnectionProperties)

    val tableProps = MysqlConnector.getRows(s"SHOW TABLE STATUS WHERE Name = '${CreateTableOptionsTable}'", (resultSet) => {
      (resultSet.getString("Engine"), resultSet.getString("Collation"), resultSet.getString("Comment"))
    })
    tableProps should have size 1
    val (engine, collation, comment) = tableProps(0)
    engine shouldEqual "InnoDB"
    collation shouldEqual "utf8_general_ci"
    comment shouldEqual "Spark SQL test table"
  }

  "createTableColumnTypes" should "add custom VARCHAR length to created field" in {
    val users = (1 to 10).map(nr => User(nr, s"User#${nr}")).toDF("id", "login")

    users.write
      // We can't do more, e.g. specify `id INT(11) NOT NULL PRIMARY KEY`
      .option("createTableColumnTypes", "id SMALLINT, login VARCHAR(7)")
      .mode(SaveMode.Overwrite)
      .jdbc(Connection, CreateTableColumnTypesTable, ConnectionProperties)

    val tableProps = MysqlConnector.getRows(s"DESC ${CreateTableColumnTypesTable}", (resultSet) => {
      (resultSet.getString("Field"), resultSet.getString("Type"))
    })
    tableProps should have size 2
    tableProps should contain allOf(("id", "smallint(6)"), ("login", "varchar(7)"))
  }

  "customSchema" should "convert number type to string at reading" in {
    MysqlConnector.executedSideEffectQuery(s"CREATE TABLE ${CustomSchemaTable} (id INT(11) NOT NULL, login TEXT)")
    val usersToInsert = (1 to 3).map(nr => User(nr, s"User#${nr}"))
    MysqlConnector.populateTable(s"INSERT INTO ${CustomSchemaTable} (id, login) VALUES (?, ?)", usersToInsert)
    val users = sparkSession.read.format("jdbc").option("url", Connection)
      .option("dbtable", CustomSchemaTable)
      .option("user", DbUser)
      .option("password", DbPassword)
      .option("customSchema", "id STRING, name STRING")
      .load()

    users.schema.fields(0).dataType shouldBe a [StringType]
    users.schema.fields(1).dataType shouldBe a [StringType]
    val userIds = users.map(row => row.getAs[String]("id")).collectAsList()
    userIds should contain allOf("1", "2", "3")
  }

  "batchsize of 3" should "insert 10 rows in 4 batches" in {
    val users = (1 to 10).map(nr => User(nr, s"User#${nr}")).toDF("id", "login")
      // We test batchsize so having 1 partition is more than useful
      .repartition(1)

    MethodInvocationDecorator.decorateClass("com.mysql.cj.jdbc.StatementImpl", "executeBatch").toClass
    users.write
      .option("batchsize", 3)
      .mode(SaveMode.Overwrite)
      .jdbc(Connection, BatchSizeTable, ConnectionProperties)

    MethodInvocationCounter.methodInvocations(ExecuteBatchMethodName) shouldEqual 4
    val rows = MysqlConnector.getRows(s"SELECT * FROM ${BatchSizeTable}", (resultSet) => {
      (resultSet.getInt("id"), resultSet.getString("login"))
    })
    rows should have size 10
    rows should contain allOf((1, "User#1"), (2, "User#2"), (3, "User#3"), (4, "User#4"),
      (5, "User#5"), (6, "User#6"), (7, "User#7"), (8, "User#8"), (9, "User#9"), (10, "User#10"))
  }

  "sessionInitStatement" should "remove one row before adding new rows from DataFrame" in {
    MysqlConnector.executedSideEffectQuery(s"CREATE TABLE IF NOT EXISTS ${SessionInitTable} " +
      s"(id INT(11) NOT NULL, login TEXT)")
    val usersToInsert = (1 to 2).map(nr => User(nr, s"User#${nr}"))
    MysqlConnector.populateTable(s"INSERT INTO ${SessionInitTable} (id, login) VALUES (?, ?)", usersToInsert)

    val users = sparkSession.read.format("jdbc").option("url", Connection)
      .option("dbtable", SessionInitTable)
      .option("user", DbUser)
      .option("password", DbPassword)
      .option("sessionInitStatement", s"DELETE FROM ${SessionInitTable} WHERE id < 2")
      .load()
      .select("id")

    val usersIds = users.collect().map(row => row.getAs[Int]("id"))
    usersIds should have size 1
    usersIds(0) shouldEqual 2
    val rows = MysqlConnector.getRows(s"SELECT * FROM ${SessionInitTable}", (resultSet) => {
      (resultSet.getInt("id"), resultSet.getString("login"))
    })
    rows should have size 1
    rows(0) shouldEqual (2, "User#2")
  }
}

case class User(id: Long, login: String) extends DataOperation {
  override def populatePreparedStatement(preparedStatement: PreparedStatement): Unit = {
    preparedStatement.setLong(1, id)
    preparedStatement.setString(2, login)
  }
}
