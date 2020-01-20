package com.waitingforcode.sql

import com.waitingforcode.util.InMemoryLogAppender
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ReorderJoinTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("Reorder JOIN test")
    .master("local[*]").getOrCreate()
  import sparkSession.implicits._

  behavior of "logical reorder join"

  it should "apply to 3 tables joined from a SELECT clause" in {
    val logAppender = InMemoryLogAppender.createLogAppender(
      Seq("Applying Rule org.apache.spark.sql.catalyst.optimizer.ReorderJoin"))

    val users = (0 to 100).map(nr => (nr, s"user#${nr}")).toDF("id", "login")
    users.createTempView("users_list")
    val actions = (0 to 100).flatMap(userId => {
      (0 to 100).map(actionNr => (userId, s"action${actionNr}"))
    }).toDF("action_user", "action_name")
    actions.createTempView("users_actions")
    val usersLogged = (0 to 100 by 2).map(nr => (nr, System.currentTimeMillis())).toDF("logged_user", "last_login")
    usersLogged.createTempView("users_logged")

    sparkSession.sql(
      """
        |SELECT ul.*, ua.*, ulo.*
        |FROM users_list AS ul, users_actions AS ua, users_logged AS ulo
        |WHERE ul.id = ua.action_user AND ulo.logged_user = ul.id
      """.stripMargin).explain(true)

    logAppender.getMessagesText() should have size 1
    logAppender.getMessagesText()(0).trim should startWith("=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ReorderJoin ===")
  }

  it should "apply to 3 tables joined with a WHERE clause" in {
    val logAppender = InMemoryLogAppender.createLogAppender(
      Seq("Applying Rule org.apache.spark.sql.catalyst.optimizer.ReorderJoin"))

    val users = (0 to 100).map(nr => (nr, s"user#${nr}")).toDF("id", "login")
    users.createTempView("users_list")
    val actions = (0 to 100).flatMap(userId => {
      (0 to 100).map(actionNr => (userId, s"action${actionNr}"))
    }).toDF("action_user", "action_name")
    actions.createTempView("users_actions")
    val usersLogged = (0 to 100 by 2).map(nr => (nr, System.currentTimeMillis())).toDF("logged_user", "last_login")
    usersLogged.createTempView("users_logged")

    sparkSession.sql(
      """
        |SELECT ul.*, ua.*, ulo.*
        |FROM users_list AS ul JOIN users_actions AS ua JOIN users_logged AS ulo
        |WHERE ul.id = ua.action_user AND ulo.logged_user = ul.id
      """.stripMargin).explain(true)

    logAppender.getMessagesText() should have size 1
    logAppender.getMessagesText()(0).trim should startWith("=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ReorderJoin ===")
  }

  it should "leave a predicate that cannot be pushed into ON clause as a separate filter" in {
    val logAppender = InMemoryLogAppender.createLogAppender(
      Seq("Applying Rule org.apache.spark.sql.catalyst.optimizer.ReorderJoin"))

    val users = (0 to 100).map(nr => (nr, s"user#${nr}")).toDF("id", "login")
    users.createTempView("users_list")
    val actions = (0 to 100).flatMap(userId => {
      (0 to 100).map(actionNr => (userId, s"action${actionNr}"))
    }).toDF("action_user", "action_name")
    actions.createTempView("users_actions")
    val usersLogged = (0 to 100 by 2).map(nr => (nr, System.currentTimeMillis())).toDF("logged_user", "last_login")
    usersLogged.createTempView("users_logged")
    (0 to 100).toDF("number").createTempView("xyz")

    sparkSession.sql(
      """
        |SELECT ul.*, ua.*, ulo.*
        |FROM users_list AS ul, users_actions AS ua, users_logged AS ulo
        |WHERE ul.id = ua.action_user AND ulo.logged_user = ul.id AND ul.id IN (SELECT number FROM xyz)
      """.stripMargin).explain(true)

    logAppender.getMessagesText() should have size 1
    logAppender.getMessagesText()(0).trim should include("+- Filter id#5 IN (list#32 [])")
  }

  it should "not apply to 2 tables joined with a WHERE clause" in {
    val logAppender = InMemoryLogAppender.createLogAppender(
      Seq("Applying Rule org.apache.spark.sql.catalyst.optimizer.ReorderJoin"))

    val users = (0 to 100).map(nr => (nr, s"user#${nr}")).toDF("id", "login")
    users.createTempView("users_list")
    val actions = (0 to 100).flatMap(userId => {
      (0 to 100).map(actionNr => (userId, s"action${actionNr}"))
    }).toDF("action_user", "action_name")
    actions.createTempView("users_actions")

    sparkSession.sql(
      """
        |SELECT ul.*, ua.*
        |FROM users_list AS ul JOIN users_actions AS ua
        |WHERE ul.id = ua.action_user
      """.stripMargin).explain(true)

    logAppender.getMessagesText() shouldBe empty
  }



}
