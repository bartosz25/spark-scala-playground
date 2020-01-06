package com.waitingforcode.sql

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.{FlatSpec, Matchers}

class DateTimeFunctionsTest extends FlatSpec with Matchers {

  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL date time functions")
    .master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  "add_months" should "add 2 months to the date column" in {
    val dataset = Seq(("2019-09-20"), ("2019-01-20"), ("2019-13-01")).toDF("datetime_col")

    val result = dataset.select(functions.add_months($"datetime_col", 2).as("date_with_2_months").cast("string"))
      .map(row => row.getAs[String]("date_with_2_months")).collect()

    result should have size 3
    result should contain allOf("2019-11-20", "2019-03-20", null)
    dataset.select(functions.add_months($"datetime_col", 2).as("date_with_2_months")).printSchema()
  }

  "current_date" should "return current date" in {
    val now = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)
    val dataset = Seq(("2019-09-20"), ("2019-01-20")).toDF("datetime_col")

    val result = dataset.select(functions.current_date().as("current_date").cast("string"))
      .map(row => row.getAs[String]("current_date")).collect()

    result should have size 2
    result should contain only (now)
    dataset.select(functions.current_date().as("current_date")).printSchema()
  }

  "current_timestamp" should "return current timestamp" in {
    val now = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME).dropRight(8).replace("T", " ")
    val dataset = Seq(("2019-09-20"), ("2019-01-20")).toDF("datetime_col")

    val result = dataset.select(functions.current_timestamp().as("current_timestamp").cast("string"))
      .map(row => row.getAs[String]("current_timestamp").dropRight(8)).collect()

    result should have size 2
    result should contain only (now)
  }

  "current_timestamp" should "return the same timestamp for all rows" in {
    val result = (0 to 1000).toDF("x").select(functions.current_timestamp().as("current_timestamp"))
      .map(row => row.getAs[Timestamp]("current_timestamp").getTime).collect.toSet

    result should have size 1
  }

  "date_add" should "add different number of days to a date" in {
    val dataset = Seq(("2019-09-20"), ("2019-01-20")).toDF("datetime_col")

    val result = dataset.select(functions.date_add($"datetime_col", 45).as("date_in_45_days").cast("string"))
      .map(row => row.getAs[String]("date_in_45_days")).collect()

    result should have size 2
    result should contain allOf("2019-11-04", "2019-03-06")
  }

  "date_format" should "reformat date into a custom format" in {
    val dataset = Seq(("2019-09-20"), ("2019-01-20T20:00")).toDF("datetime_col")

    val result = dataset.select(functions.date_format($"datetime_col", "dd-MM-yyyy 'at' HH").as("formatted_date").cast("string"))
      .map(row => row.getAs[String]("formatted_date")).collect()

    result should have size 2
    result should contain allOf("20-09-2019 at 00", "20-01-2019 at 20")
  }

  "date_sub" should "substract 45 days from the input date" in {
    val dataset = Seq(("2019-09-20"), ("2019-01-20")).toDF("datetime_col")

    val result = dataset.select(functions.date_sub($"datetime_col", 45).as("date_45_days_ago").cast("string"))
      .map(row => row.getAs[String]("date_45_days_ago")).collect()

    result should have size 2
    result should contain allOf("2019-08-06", "2018-12-06")
  }

  "date_trunc" should "truncate timestamp to the first hour of the day" in {
    val dataset = Seq(
      ("2019-09-20T00:30"),
      ("2011-10-20T07:50"),
      ("2020-02-29")
    ).toDF("datetime_col")

    val result = dataset.select(functions.date_trunc("hour", $"datetime_col" ).as("truncated_datetime").cast("string"))
      .map(row => row.getAs[String]("truncated_datetime")).collect()

    result should have size 3
    result should contain allOf("2019-09-20 00:00:00", "2011-10-20 07:00:00", "2020-02-29 00:00:00")
  }

  "datediff" should "compute difference between 2 dates" in {
    val dataset = Seq(
      ("2019-09-20T00:00", "2019-09-20T23:00"), // same day; will be 0 since not rounded
      ("2019-01-20T10:00", "2019-09-24T20:00"), // the 2nd col greater
      ("2019-09-20T20:00", "2019-01-20T03:00") // the 1st col greater
    ).toDF("datetime_col1", "datetime_col2")

    val result = dataset.select(functions.datediff($"datetime_col1", $"datetime_col2").as("diff"))
      .map(row => row.getAs[Integer]("diff")).collect()

    result should have size 3
    result should contain allOf(0, -247, 243)
  }

  "dayofmonth" should "extract day of month" in {
    val dataset = Seq(("2019-09-20"),
      ("2020-02-29"), // leap day
      ("2020-02-30") // doesn't exist
    ).toDF("datetime_col")

    val result = dataset.select(functions.dayofmonth($"datetime_col").as("date_dayofmonth").cast("string"))
      .map(row => row.getAs[String]("date_dayofmonth")).collect()

    result should have size 3
    result should contain allOf("20", "29", null) // null because 30/02/2020 doesn't exist
  }

  "dayofweek" should "extract day of week" in {
    val dataset = Seq(
      ("2019-09-20"),
      ("2019-09-15"), // Sunday
      ("2019-09-16") // Monday
    ).toDF("datetime_col")

    val result = dataset.select(functions.dayofweek($"datetime_col").as("date_dayofweek").cast("string"))
      .map(row => row.getAs[String]("date_dayofweek")).collect()

    result should have size 3
    result should contain allOf("6", "1", "2")

  }

  "dayofyear" should "extract day of year" in {
    val dataset = Seq(("2019-09-20"),
      ("2020-02-29"), // leap day
      ("2020-02-30") // doesn't exist
    ).toDF("datetime_col")

    val result = dataset.select(functions.dayofyear($"datetime_col").as("date_dayofyear").cast("string"))
      .map(row => row.getAs[String]("date_dayofyear")).collect()

    result should have size 3
    result should contain allOf("263", "60", null) // null because 30/02/2020 doesn't exist
  }

  "from_unixtime" should "convert unix seconds to a datetime" in {
    val dataset = Seq(
      (0),
      (10)
    ).toDF("datetime_col")

    val result = dataset.select(functions.from_unixtime($"datetime_col")
      .as("string_datetime").cast("string"))
      .map(row => row.getAs[String]("string_datetime")).collect()

    result should have size 2
    result should contain allOf("1970-01-01 01:00:00", "1970-01-01 01:00:10")
  }

  "from_utc_timestamp" should "convert datetime to Europe/Paris timezone" in {
    val dataset = Seq(
      ("2018-11-10T20:30"),
      ("2019-09-20T00:30")
    ).toDF("datetime_col")

    val result = dataset.select(functions.from_utc_timestamp($"datetime_col", "Europe/Paris")
      .as("paris_datetime").cast("string"))
      .map(row => row.getAs[String]("paris_datetime")).collect()

    result should have size 2
    result should contain allOf("2018-11-10 21:30:00", "2019-09-20 02:30:00")
  }

  "hour" should "extract hour from datetime" in {
    val dataset = Seq(
      ("2019-09-20T00:00"),
      ("2019-01-20T07:00"),
      ("2019-09-20T20:00"), // will return PM/AM or 24-clock version?,
      ("2019-01-20") // what will be default for this?
    ).toDF("datetime_col")

    val result = dataset.select(functions.hour($"datetime_col").as("hour"))
      .map(row => row.getAs[Integer]("hour")).collect()

    result should have size 4
    result should contain allOf(0, 7, 20)
  }

  "last_day" should "return the last day of the month" in {
    val dataset = Seq(
      ("2019-09-20T00:30"),
      ("2019-02-20T07:50"),
      ("2020-02-20") // leap year
    ).toDF("datetime_col")

    val result = dataset.select(functions.last_day($"datetime_col").as("last_day_of_month").cast("string"))
      .map(row => row.getAs[String]("last_day_of_month")).collect()

    result should have size 3
    result should contain allOf("2019-09-30", "2019-02-28", "2020-02-29")
  }

  "minute" should "extract minute from the datetime" in {
    val dataset = Seq(
      ("2019-09-20T00:30"),
      ("2019-01-20T07:50"),
      ("2019-01-20")
    ).toDF("datetime_col")

    val result = dataset.select(functions.minute($"datetime_col").as("minute"))
      .map(row => row.getAs[Integer]("minute")).collect()

    result should have size 3
    result should contain allOf(30, 50, 0)
  }

  "month" should "extract month from the input dates" in {
    val dataset = Seq(("2019-09-20"), ("2013-01-20")).toDF("datetime_col")

    val result = dataset.select(functions.month($"datetime_col").as("date_month").cast("string"))
      .map(row => row.getAs[String]("date_month")).collect()

    result should have size 2
    result should contain allOf("1", "9")
  }

  "months_between" should "return difference between 2 dates" in {
    val dataset = Seq(
      ("2019-09-20T00:00", "2019-09-20T23:00"), // same day; will be 0 since not rounded
      ("2019-09-20T00:00", "2019-09-21T23:00"), // same month
      ("2019-01-20T10:00", "2019-09-24T20:00"), // the 2nd col greater
      ("2019-09-20T20:00", "2019-01-20T03:00"), // the 1st col greater; same day of month
      ("2019-09-16", "2019-09-01") // 15 days of difference, so 15/31 ~ 0.5 months of difference (0.48387097 exactly)
    ).toDF("datetime_col1", "datetime_col2")

    val result = dataset.select(functions.months_between($"datetime_col1", $"datetime_col2").as("diff"))
      .map(row => row.getAs[Double]("diff")).collect()

    result should have size 5
    result should contain allOf(0.0, -0.06317204, -8.14247312, 8.0, 0.48387097)
  }

  "next_day" should "return next Monday for the date" in {
    val dataset = Seq(
      ("2019-09-20T00:30"),
      ("2019-02-20T07:50"),
      ("2019-01-20")
    ).toDF("datetime_col")

    val result = dataset.select(functions.next_day($"datetime_col", "Mon").as("next_monday").cast("string"))
      .map(row => row.getAs[String]("next_monday")).collect()

    result should have size 3
    result should contain allOf("2019-09-23", "2019-02-25", "2019-01-21")
  }

  "quarter" should "extract quarter from the date" in {
    val dataset = Seq(("2019-09-20"), ("2013-01-20")).toDF("datetime_col")

    val result = dataset.select(functions.quarter($"datetime_col").as("date_quarter").cast("string"))
      .map(row => row.getAs[String]("date_quarter")).collect()

    result should have size 2
    result should contain allOf("1", "3")
  }

  "second" should "extract seconds from the datetime" in {
    val dataset = Seq(
      ("2019-09-20T00:30:30"),
      ("2019-01-20T07:50:02"),
      ("2019-01-20")
    ).toDF("datetime_col")

    val result = dataset.select(functions.second($"datetime_col").as("second"))
      .map(row => row.getAs[Integer]("second")).collect()

    result should have size 3
    result should contain allOf(30, 2, 0)
  }

  "to_date" should "convert a string field into a date" in {
    val dataset = Seq(
      ("2018-11-10 20:30"),
      ("2019-09-20 00:30")
    ).toDF("datetime_col")

    // also exists a to_date(col) without the format
    val result = dataset.select(functions.to_date($"datetime_col", "yyyy-MM-dd HH:mm")
      .as("date"))

    result.schema.toString shouldEqual "StructType(StructField(date,DateType,true))"
    val stringDates = result.select($"date".cast("string")).map(row => row.getAs[String]("date")).collect()
    stringDates should have size 2
    stringDates should contain allOf("2018-11-10", "2019-09-20")
  }

  "to_timestamp" should "convert a string into timestamp type" in {
    val dataset = Seq(
      ("2018-11-10 20:30"),
      ("2019-09-20 00:30")
    ).toDF("datetime_col")

    // also exists a to_timestamp(col) without the format
    val result = dataset.select(functions.to_timestamp($"datetime_col", "yyyy-MM-dd HH:mm")
      .as("timestamp"))

    result.schema.toString shouldEqual "StructType(StructField(timestamp,TimestampType,true))"
    val stringDates = result.select($"timestamp".cast("string")).map(row => row.getAs[String]("timestamp")).collect()
    stringDates should have size 2
    stringDates should contain allOf("2018-11-10 20:30:00", "2019-09-20 00:30:00")
  }

  "to_utc_timestamp" should "convert Europe/Paris datetime into UTC format" in {
    val dataset = Seq(
      ("2018-11-10T20:30"),
      ("2019-09-20T00:30")
    ).toDF("datetime_col")

    val result = dataset.select(functions.to_utc_timestamp($"datetime_col", "Europe/Paris")
      .as("utc_datetime").cast("string"))
      .map(row => row.getAs[String]("utc_datetime")).collect()

    result should have size 2
    result should contain allOf("2018-11-10 19:30:00", "2019-09-19 22:30:00")
  }

  "trunc" should "truncate date to the first day of the month" in {
    val dataset = Seq(
      ("2019-09-20T00:30"),
      ("2011-10-20T07:50"),
      ("2020-02-29")
    ).toDF("datetime_col")

    val result = dataset.select(functions.trunc($"datetime_col", "month").as("truncated_date").cast("string"))
      .map(row => row.getAs[String]("truncated_date")).collect()

    result should have size 3
    result should contain allOf("2019-09-01", "2011-10-01", "2020-02-01")
  }

  "unix_timestamp" should "return Unix timestamp for every row" in {
    // version without the parameter returns the current time
    val dataset = Seq(
      ("2018-11-10 20:30:30"),
      ("2019-09-20T00:30")
    ).toDF("datetime_col")

    val result = dataset.select(functions.unix_timestamp($"datetime_col")
      .as("timestamp")).collect().mkString(", ")

    result shouldEqual "[1541878230], [null]" // null because the input date was malformated
  }

  "year" should "extract the year from the input dates" in {
    val dataset = Seq(("2019-09-20"), ("2013-01-20")).toDF("datetime_col")

    val result = dataset.select(functions.year($"datetime_col").as("date_year"))
      .map(row => row.getAs[Integer]("date_year")).collect()

    result should have size 2
    result should contain allOf(2019, 2013)
  }

  "weekofyear" should "return week of year" in {
    val dataset = Seq(
      ("2019-09-20T00:30"),
      ("2011-10-20T07:50"),
      ("2020-02-29")
    ).toDF("datetime_col")

    val result = dataset.select(functions.weekofyear($"datetime_col").as("date_weekofyear"))
      .map(row => row.getAs[Integer]("date_weekofyear")).collect()

    result should have size 3
    result should contain allOf(38, 42, 9)
  }

  "window" should "build 3 1-minute windows" in {
    val dataset = Seq(
      ("2019-09-20T00:30"),
      ("2011-10-20T07:50"),
      ("2011-10-20T07:50"),
      ("2020-02-29")
    ).toDF("datetime_col")

    val result = dataset.select(functions.window($"datetime_col", "1 minute").as("1_min_window").cast("string"))
      .map(row => row.getAs[String]("1_min_window")).collect()

    result should have size 4
    result should contain allElementsOf (Seq(
      "[2019-09-20 00:30:00, 2019-09-20 00:31:00]",
      "[2011-10-20 07:50:00, 2011-10-20 07:51:00]",
      "[2011-10-20 07:50:00, 2011-10-20 07:51:00]", // As you can see, it includes duplicates
      "[2020-02-29 00:00:00, 2020-02-29 00:01:00]"
    ))
  }

}

