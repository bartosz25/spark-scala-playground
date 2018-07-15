package com.waitingforcode.tips

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class NestedDataManipulationTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL nested data manipulation tip").master("local[*]").getOrCreate()

  override def afterAll {
    sparkSession.stop()
  }

  "Row API" should "be used to manipulate nested data" in {
    import sparkSession.implicits._
    val usersVisits = Seq(
      ("u1", Browser("Firefox", "1.23", "en-EN")),
      ("u2", Browser("Firefox", "1.23", "fr-FR")),
      ("u3", Browser("Internet Explorer", "11.23", "en-EN")),
      ("u4", Browser("Chrome", "16.23", "en-EN"))
    ).toDF("user_id", "browser")


    val visitCodes = usersVisits.map(row => {
      val browserContext = row.getAs[Row]("browser")
      s"${row.getAs[String]("user_id")}-${browserContext.getAs[String]("name")}-${browserContext.getAs[String]("version")}"
    }).collectAsList()

    visitCodes should have size 4
    visitCodes should contain allOf("u1-Firefox-1.23", "u2-Firefox-1.23", "u3-Internet Explorer-11.23",
      "u4-Chrome-16.23")
  }

}

case class Browser(name: String, version: String, lang: String)