package com.waitingforcode.sql

import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.{FlatSpec, Matchers}


class LessKnownAggregationFunctionsTest extends FlatSpec with Matchers {

  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL less known aggregations")
    .master("local[*]")
    .getOrCreate()

  "collect_list" should "collect all values into a list" in {
    import sparkSession.implicits._
    val dataset = Seq(
      (1, "a"), (1, "aa"), (1, "a"), (2, "b"), (2, "b"), (3, "c"), (3, "c")
    ).toDF("nr", "letter")

    val result = dataset.groupBy("nr").agg(functions.collect_list("letter").as("collected_letters"))
      .map(row =>
        (row.getAs[Integer]("nr"), row.getAs[Seq[String]]("collected_letters"))
      ).collect()

    result should have size 3
    result should contain allOf(
      (1, Seq("a", "aa", "a")),
      (2, Seq("b", "b")),
      (3, Seq("c", "c"))
    )
  }

  "collect_set" should "collect all values into a set and therefore, drop duplicates" in {
    import sparkSession.implicits._
    val dataset = Seq(
      (1, "a"), (1, "aa"), (1, "a"), (2, "b"), (2, "b"), (3, "c"), (3, "c")
    ).toDF("nr", "letter")

    val result = dataset.groupBy("nr").agg(functions.collect_set("letter").as("collected_letters"))
      .map(row =>
        (row.getAs[Integer]("nr"), row.getAs[Seq[String]]("collected_letters"))
      ).collect()

    result should have size 3
    result should contain allOf(
      (1, Seq("a", "aa")),
      (2, Seq("b")),
      (3, Seq("c"))
    )
  }

  "corr" should "compute correlation between the 2 columns in a dataset" in {
    import sparkSession.implicits._
    val dataset = Seq(
      (1, 1, 10), (1, 2, 20), (1, 3, 30), // positive correlation
      (2, 1, 2), (2, 2, 1), (2, 3, 3), (2, 4, 2),  (2, 4, 2),  (2, 4, 2),  (2, 4, 2),  (2, 4, 2),
      (3, 31, 15), (3, 32, 14) // negative correlation
    ).toDF("nr1", "nr2", "nr3")

    val result = dataset.groupBy("nr1").agg(functions.corr("nr2", "nr3").as("correlation"))
      .map(row => (row.getAs[Int]("nr1"), row.getAs[Double]("correlation")))
      .collect()

    result should have size 3
    result should contain allOf(
      (1, 1.0d),
      (2, 0.22941573387056163d),
      (3, -1.0d)
    )
  }

  "kurtosis" should "compute the heaviness of the distribution tails" in {
    import sparkSession.implicits._
    val dataset = Seq(
      (1, 1), (1, 2), (1, 3),
      (2, 1), (2, 2), (2, 3), (2, 4), (2, 5), (2, 6), (2, 7), (2, 8), (2, 9), (2, 10), (2, 11), (2, 12),
      (3, 133), (3, 230), (3, 300),(3, 300),(3, 300),(3, 300),(3, 300), (3, 300),(3, 300),(3, 300),(3, 300),(3, 300)
    ).toDF("nr1", "nr2")

    val result = dataset.groupBy("nr1").agg(functions.kurtosis("nr2" ).as("kurtosis"))
      .map(row => (row.getAs[Int]("nr1"), row.getAs[Double]("kurtosis")))
      .collect()

    result should have size 3
    result should contain allOf(
      (1, -1.5d),
      (2, -1.2167832167832169d),
      (3, 4.2605520290939705d)
    )
  }

  "covar_pop" should "compute covariance for 2 columns of a dataset" in {
    import sparkSession.implicits._
    val dataset = Seq(
      (1, 1, 10), (1, 2, 20), (1, 3, 30),
      (2, 1, 20), (2, 2, 19), (2, 3, 10), (2, 15, 10),
      (3, 31, 15), (3, 30, 14), (3, 29, 24), (3, 36, 24)
    ).toDF("nr1", "nr2", "nr3")

    val result = dataset.groupBy("nr1").agg(functions.covar_pop("nr2", "nr2").as("covar_pop"))
      .map(row => (row.getAs[Int]("nr1"), row.getAs[Double]("covar_pop")))
      .collect()

    result should have size 3
    result should contain allOf(
      (1, 0.6666666666666666d),
      (2, 32.1875d),
      (3, 7.25d)
    )
  }

  "covar_samp" should "compute sample covariance for 2 columns of a dataset" in {
    import sparkSession.implicits._
    val dataset = Seq(
      (1, 1, 10), (1, 2, 20), (1, 3, 30),
      (2, 1, 20), (2, 2, 19), (2, 3, 10), (2, 15, 10),
      (3, 31, 15), (3, 30, 14), (3, 29, 24), (3, 36, 24)
    ).toDF("nr1", "nr2", "nr3")

    val result = dataset.groupBy("nr1").agg(functions.covar_samp("nr2", "nr2").as("covar_samp"))
      .map(row => (row.getAs[Int]("nr1"), row.getAs[Double]("covar_samp")))
      .collect()

    result should have size 3
    result should contain allOf(
      (1, 1.0d),
      (2, 42.916666666666664d),
      (3, 9.666666666666666d)
    )
  }

  "stddev_samp" should "compute sample standard deviation" in {
    import sparkSession.implicits._
    val dataset = Seq(
      (1, 1), (1, 2), (1, 3),
      (2, 3), (2, 6), (2, 9), (2, 12)
    ).toDF("nr1", "nr2")

    val result = dataset.groupBy("nr1").agg(functions.stddev_samp("nr2" ).as("ssd"))
      .map(row => (row.getAs[Int]("nr1"), row.getAs[Double]("ssd")))
      .collect()

    result should have size 2
    result should contain allOf(
      (1, 1.0d),
      // Why not 3? Let's compute it with the formulas defined
      // in https://www.mathsisfun.com/data/standard-deviation-formulas.html
      // 1. Mean: (3 + 6 + 9 + 12) / 4 = 30 / 4 = 7.5
      // 2. For each number subtract the mean and square the result:
      // (3 - 7.5)^2 = 20.25
      // (6 - 7.5)^2 = 2.25
      // (9 - 7.5)^2 = 2.25
      // (12 - 7.5)^2 = 20.25
      // 3.work out the mean of those squared differences:
      // 20.25 + 2.25 + 2.25 + 20.25 = 45
      // 1/3 * 45 = 15
      // 4. Take the square root of that
      // √45 = 3.872983346207417
      (2, 3.872983346207417d)
    )
  }

  "stddev_pop" should "compute standard deviation" in {
    import sparkSession.implicits._
    val dataset = Seq(
      (1, 1), (1, 2), (1, 3),
      (2, 3), (2, 6), (2, 9), (2, 12)
    ).toDF("nr1", "nr2")

    val result = dataset.groupBy("nr1").agg(functions.stddev_pop("nr2" ).as("ssp"))
      .map(row => (row.getAs[Int]("nr1"), row.getAs[Double]("ssp")))
      .collect()

    result should have size 2
    result should contain allOf(
      (1, 0.816496580927726d),
      (2, 3.3541019662496847d)
    )
  }

  "var_samp" should "compute sample variance of the dataset" in {
    import sparkSession.implicits._
    val dataset = Seq(
      (1, 1), (1, 2), (1, 3),
      (2, 3), (2, 6), (2, 9), (2, 12)
    ).toDF("nr1", "nr2")

    val result = dataset.groupBy("nr1").agg(functions.var_samp("nr2" ).as("var_samp"))
      .map(row => (row.getAs[Int]("nr1"), row.getAs[Double]("var_samp")))
      .collect()

    result should have size 2
    result should contain allOf(
      (1, 1d),
      (2, 15d)
    )
  }

  "var_pop" should "compute population variance of the dataset" in {
    import sparkSession.implicits._
    val dataset = Seq(
      (1, 1), (1, 2), (1, 3),
      (2, 3), (2, 6), (2, 9), (2, 12)
    ).toDF("nr1", "nr2")

    val result = dataset.groupBy("nr1").agg(functions.var_pop("nr2" ).as("var_pop"))
      .map(row => (row.getAs[Int]("nr1"), row.getAs[Double]("var_pop")))
      .collect()

    result should have size 2
    result should contain allOf(
      (1, 0.6666666666666666d),
      (2, 11.25d)
    )
  }

  "skewness" should "compute dataset distribution" in {
    import sparkSession.implicits._
    val dataset = Seq(
      (1, 1), (1, 2), (1, 3), // symmetry
      (2, 3), (2, 23), (2, 1), (2, 50), // right skew
      (3, 3), (3, -23), (3, 1), (3, -50) // left skew
    ).toDF("nr1", "nr2")

    val result = dataset.groupBy("nr1").agg(functions.skewness("nr2" ).as("skewness"))
      .map(row => (row.getAs[Int]("nr1"), row.getAs[Double]("skewness")))
      .collect()

    result should have size 3
    result should contain allOf(
      (1, 0.0d),
      (2, 0.6108295646625682),
      (3, -0.5267113956527112)
    )
  }

  "first" should "return the first value from the aggregated group" in {
    import sparkSession.implicits._
    val dataset = Seq(
      (1, 11), (1, 12), (1, 13), (2, 21), (2, 22), (3, 31), (3, 32)
    ).toDF("nr1", "nr2")

    val result = dataset.groupBy("nr1").agg(functions.first("nr2").as("first_value")).collect()

    // first is not deterministic, so it can return different values for consecutive runs
    // That's why I'm making here a more complicated assertion on the content
    result should have size 3
    val acceptableValues = Map(1 -> Set(11, 12, 13), 2 -> Set(21, 22), 3 -> Set(31, 32))
    result.foreach(row => {
      val id = row.getAs[Int]("nr1")
      val firstNumber = row.getAs[Int] ("first_value")
      acceptableValues(id) should contain (firstNumber)
    })
  }

  "last" should "return the last value from the aggregated group" in {
    import sparkSession.implicits._
    val dataset = Seq(
      (1, 11), (1, 12), (1, 13), (2, 21), (2, 22), (3, 31), (3, 32)
    ).toDF("nr1", "nr2")

    val result = dataset.groupBy("nr1").agg(functions.last("nr2").as("last_value")).collect()

    // first is not deterministic, so it can return different values for consecutive runs
    // That's why I'm making here a more complicated assertion on the content
    result should have size 3
    val acceptableValues = Map(1 -> Set(11, 12, 13), 2 -> Set(21, 22), 3 -> Set(31, 32))
    result.foreach(row => {
      val id = row.getAs[Int]("nr1")
      val lastValue = row.getAs[Int] ("last_value")
      acceptableValues(id) should contain (lastValue)
    })
  }

}
