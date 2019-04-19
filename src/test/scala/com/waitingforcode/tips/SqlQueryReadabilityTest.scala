package com.waitingforcode.tips

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class SqlQueryReadabilityTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL raw interpolator tip").master("local[*]").getOrCreate()

  override def afterAll {
    sparkSession.stop()
  }

  "Firefox query" should "return 2 users" in {
    import sparkSession.implicits._
    val usersVisits = Seq(
      ("u1", VisitBrowser("Firefox", "1.23", "en-EN")),
      ("u2", VisitBrowser("Firefox", "1.23", "fr-FR")),
      ("u3", VisitBrowser("Firefox", "1.23", "en-EN")),
      ("u4", VisitBrowser("Chrome", "16.23", "en-EN"))
    ).toDF("user_id", "browser")
    val userClicks = Seq(
      ("u1"), ("u2")
    ).toDF("user_click_id")
    usersVisits.createOrReplaceTempView("visits")
    userClicks.createOrReplaceTempView("clicks")

    val firefoxVisitsWithClicks = sparkSession.sql(
      """SELECT v.* FROM visits v
        |JOIN clicks c ON c.user_click_id = v.user_id
        |WHERE v.browser.name = "Firefox"""".stripMargin
    )
    val firefoxVisitsWithClicksNoTripleQuotes = sparkSession.sql(
      "SELECT v.* FROM visits v JOIN clicks c ON " +
      "c.user_click_id = v.user_id WHERE v.browser.name = \"Firefox\""
    )

    firefoxVisitsWithClicks.map(row => row.getAs[String]("user_id")).collect() should contain allOf("u1", "u2")
    firefoxVisitsWithClicksNoTripleQuotes.map(row => row.getAs[String]("user_id")).collect() should contain allOf("u1", "u2")
  }

}
case class VisitBrowser(name: String, version: String, lang: String)