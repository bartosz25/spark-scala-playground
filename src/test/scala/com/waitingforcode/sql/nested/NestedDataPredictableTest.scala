package com.waitingforcode.sql.nested

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

class NestedDataPredictableTest extends FlatSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  private val sparkSession: SparkSession =  SparkSession.builder()
    .appName("Spark SQL nested data test").master("local[*]").getOrCreate()
  import sparkSession.implicits._

  override def afterAll {
    sparkSession.stop()
  }

  private val OneLevelFile = "/tmp/spark/1_level.json"
  private val TwoLevelsFile = "/tmp/spark/2_levels.json"

  before {
    val oneLevelEntries =
      """
        |{"key": "event_1", "user_context": {"browser": "Firefox", "lang": "en-US", "version": "59.0.3"}}
        |{"key": "event_2", "user_context": {"browser": "Firefox", "lang": "en-US", "version": "59.0.3"}}
        |{"key": "event_3", "user_context": {"browser": "Google Chrome", "lang": "en-US", "version": "65.0.3325.181"}}
        |{"key": "event_4", "user_context": {"browser": "Google Chrome", "lang": "en-US", "version": "65.0.3325.181"}}
        |{"key": "event_5", "user_context": {"browser": "Firefox", "lang": "en-US", "version": "59.0.3"}}
        |{"key": "event_6", "user_context": {"browser": "Opera", "lang": "en-US", "version": "41.0.2353.56"}}
        |{"key": "event_7", "user_context": {"browser": "Firefox", "lang": "en-US", "version": "59.0.3"}}
      """.stripMargin
    FileUtils.writeStringToFile(new File(OneLevelFile), oneLevelEntries)
    val twoLevelsEntries =
      """
        |{"key": "event_1", "user_context": {"browser": "Firefox", "lang": "en-US", "version": "59.0.3"}, "geo": {"country": {"iso_code": "FR", "name_en": "France"}}}
        |{"key": "event_2", "user_context": {"browser": "Firefox", "lang": "en-US", "version": "59.0.3"}, "geo": {"country": {"iso_code": "FR", "name_en": "France"}}}
        |{"key": "event_3", "user_context": {"browser": "Google Chrome", "lang": "en-US", "version": "65.0.3325.181"}, "geo": {"country": {"iso_code": "UK", "name_en": "the United Kingdom "}}}
        |{"key": "event_4", "user_context": {"browser": "Google Chrome", "lang": "en-US", "version": "65.0.3325.181"}, "geo": {"country": {"iso_code": "CN", "name_en": "China"}}}
        |{"key": "event_5", "user_context": {"browser": "Firefox", "lang": "en-US", "version": "59.0.3"}, "geo": {"country": {"iso_code": "FR", "name_en": "France"}}}
        |{"key": "event_6", "user_context": {"browser": "Opera", "lang": "en-US", "version": "41.0.2353.56"}, "geo": {"country": {"iso_code": "ES", "name_en": "Spain"}}}}
        |{"key": "event_7", "user_context": {"browser": "Firefox", "lang": "en-US", "version": "59.0.3"}, "geo": {"country": {"iso_code": "IT", "name_en": "Italy"}}}
      """.stripMargin
    FileUtils.writeStringToFile(new File(TwoLevelsFile), twoLevelsEntries)
  }

  "DataFrame" should "read 1-level nested data" in {
    val oneLevelsDataFrame = sparkSession.read.json(OneLevelFile)

    val eventWithUserBrowser = oneLevelsDataFrame.select($"key", $"user_context.browser".as("user_browser"), $"user_context.lang")

    val collectedUsers = eventWithUserBrowser.collect()
      .map(row => (row.getAs[String]("key"), row.getAs[String]("user_browser"), row.getAs[String]("lang")))

    collectedUsers should have size 7
    collectedUsers should contain allOf(("event_1", "Firefox", "en-US"), ("event_2", "Firefox", "en-US"),
      ("event_3", "Google Chrome", "en-US"), ("event_4", "Google Chrome", "en-US"), ("event_5", "Firefox", "en-US"),
      ("event_6", "Opera", "en-US"), ("event_7", "Firefox", "en-US"))
  }

  "DataFrame" should "read 2-levels nested data" in {
    val twoLevelsDataFrame = sparkSession.read.json(TwoLevelsFile)

    // even for 2-level nested fields the access is direct since the used type is the structure of another structure
    val geoUserStats = twoLevelsDataFrame.select($"key", $"user_context.lang", $"geo.country.iso_code")

    val collectedUserStats = geoUserStats.collect()
      .map(row => (row.getAs[String]("key"), row.getAs[String]("lang"), row.getAs[String]("iso_code")))

    collectedUserStats should have size 7
    collectedUserStats should contain allOf(("event_1", "en-US", "FR"), ("event_2", "en-US", "FR"),
      ("event_3", "en-US", "UK"), ("event_4", "en-US", "CN"), ("event_5", "en-US", "FR"), ("event_6", "en-US", "ES"),
      ("event_7", "en-US", "IT"))
  }

}