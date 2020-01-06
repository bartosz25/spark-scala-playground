package com.waitingforcode.sql

import java.io.File
import java.sql.Timestamp

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class DateTimeConversionTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("JSON custom date format")
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  private val testInputFilePath = "/tmp/test-different-date-formats.json"

  override def beforeAll(): Unit = {
    val dateTimeLogs =
      """
        |{"key": "format_timestamp", "log_action": "2019-05-10 20:00:00.000" }
        |{"key": "format_date_without_0_in_month", "log_action": "2019-5-12" }
        |{"key": "weird_format_gmt", "log_action": "2019-01-01T00:00:00GMT+01:00"  }
        |{"key": "format_hh_mm_timezone", "log_action": "2019-01-01T00:00:00+00:00"  }
        |{"key": "format_z_timezone", "log_action": "2019-01-06T18:30:00Z"  }
        |{"key": "format_text_timezone", "log_action": "2019-01-06T18:30:00[Europe/Paris]"  }
        |{"key": "format_hh_mm_timezone_fraction_sec", "log_action": "2019-01-06T18:30:00.000+01:00"  }
        |{"key": "format_text_datetime", "log_action": "Wed, 4 Jul 2001 12:08:56 +0100"  }
        |{"key": "format_only_year_month", "log_action": "2019-01Z"  }
        |{"key": "format_hh_mm_timezone_with_text", "log_action": "2018-07-07T15:20:14.372+01:00[Europe/Paris]"  }
        |{"key": "not_existent_day", "log_action": "2019-02-30T18:30:00+01:00"  }
      """.stripMargin
    FileUtils.writeStringToFile(new File(testInputFilePath), dateTimeLogs)
  }

  override def afterAll(): Unit = {
    FileUtils.forceDelete(new File(testInputFilePath))
  }

  "JSON dates" should "be read correctly if their formats are good" in {
    val schema = StructType(Seq(
      StructField("key", StringType), StructField("log_action", TimestampType)
    ))

    val readLogs = sparkSession.read.schema(schema).json(testInputFilePath)
      .filter("key IS NOT NULL")
      .map(row => (row.getAs[String]("key"),
        row.getAs[Timestamp]("log_action").toString))
      .collect()


    sparkSession.read.schema(schema).json(testInputFilePath).show(false)
    readLogs should have size 7
    val logsByKey = readLogs.groupBy(keyLogPair => keyLogPair._1).mapValues(keyLogPairs => {
      assert(keyLogPairs.size == 1)
      keyLogPairs(0)._2
    })
    logsByKey("format_timestamp") shouldEqual "2019-05-10 20:00:00.0"
    logsByKey("format_date_without_0_in_month") shouldEqual "2019-05-12 00:00:00.0"
    logsByKey("weird_format_gmt") shouldEqual "2019-01-01 00:00:00.0"
    logsByKey("format_hh_mm_timezone") shouldEqual "2019-01-01 01:00:00.0"
    logsByKey("format_z_timezone") shouldEqual "2019-01-06 19:30:00.0"
    logsByKey("format_hh_mm_timezone_fraction_sec") shouldEqual "2019-01-06 18:30:00.0"
    logsByKey("format_hh_mm_timezone_with_text") shouldEqual "2018-07-07 16:20:14.372"
  }

  "a textual JSON date" should "be converted thanks to the custom timestampFormat" in {
    val schema = StructType(Seq(
      StructField("key", StringType), StructField("log_action", TimestampType)
    ))

    val readLogs = sparkSession.read.schema(schema)
      .option("timestampFormat", "EEE, d MMM yyyy HH:mm:ss Z").json(testInputFilePath)
      .filter("key IS NOT NULL")
      .map(row => (row.getAs[String]("key"),
        row.getAs[Timestamp]("log_action").toString))
      .collect()

    // Globally we'll get the same results as in the previous test but thanks to a
    // custom timeformat, we're also able to catch the textual datetime
    // The single difference is the absence of "format_hh_mm_timezone_with_text" which
    // cannot be converted because of the changed timestampFormat parameter
    readLogs should have size 7
    val logsByKey = readLogs.groupBy(keyLogPair => keyLogPair._1).mapValues(keyLogPairs => {
      assert(keyLogPairs.size == 1)
      keyLogPairs(0)._2
    })
    logsByKey("format_text_datetime") shouldEqual "2001-07-04 13:08:56.0"
    logsByKey("format_timestamp") shouldEqual "2019-05-10 20:00:00.0"
    logsByKey("format_date_without_0_in_month") shouldEqual "2019-05-12 00:00:00.0"
    logsByKey("weird_format_gmt") shouldEqual "2019-01-01 00:00:00.0"
    logsByKey("format_hh_mm_timezone") shouldEqual "2019-01-01 01:00:00.0"
    logsByKey("format_z_timezone") shouldEqual "2019-01-06 19:30:00.0"
    logsByKey("format_hh_mm_timezone_fraction_sec") shouldEqual "2019-01-06 18:30:00.0"
  }

}