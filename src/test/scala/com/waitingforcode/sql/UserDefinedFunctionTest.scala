package com.waitingforcode.sql

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable

class UserDefinedFunctionTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val sparkSession = SparkSession.builder().appName("UDF test")
    .master("local[*]").getOrCreate()

  def evenFlagResolver(number: Int): Boolean = {
    number%2 == 0
  }

  def multiplicatorCurried(factor: Int)(columnValue: Int): Int = {
    factor * columnValue
  }

  import sparkSession.implicits._
  val letterNumbers = Seq(
    ("A", 1), ("B", 2), ("C", 3), ("D", 4), ("E", 5), ("F", 6)
  ).toDF("letter", "number")

  val watchedMoviesPerUser = Seq(
    (1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (3, 2), (3, 4), (3, 5)
  ).toDF("user", "movie")

  val letters = Seq(
    ("a", "A"), ("b", "B"), ("c", "C"), ("d", "D"), ("e", "E"), ("f", "F")
  ).toDF("lowercased_letter", "uppercased_letter")

  override def afterAll {
    sparkSession.stop()
  }

  "UDF" should "be registered through register method" in {
    sparkSession.udf.register("EvenFlagResolver_registerTest", evenFlagResolver _)

    // function registered through udf.register can be used in select expressions
    // It could also be used with DSL, as:
    // select($"letter", udfEvenResolver($"number") as "isEven") where
    // udfEvenResolver = sparkSession.udf.register("EvenFlagResolver_registerTest", evenFlagResolver)
    val rows = letterNumbers.selectExpr("letter", "EvenFlagResolver_registerTest(number) as isEven")
      .map(row => (row.getString(0), row.getBoolean(1)))
      .collectAsList()

    rows should contain allOf(("A", false), ("B", true), ("C", false), ("D", true), ("E", false), ("F", true))
  }

  "UDF registered with udf(...)" should "not be usable in select expression" in {
    val udfEvenResolver = udf(evenFlagResolver _)

    val analysisException = intercept[AnalysisException] {
      letterNumbers.selectExpr("letter", "udfEvenResolver(number) as isEven")
        .map(row => (row.getString(0), row.getBoolean(1)))
        .collectAsList()
    }

    analysisException.message.contains("Undefined function: 'udfEvenResolver'. " +
      "This function is neither a registered temporary function nor a permanent function registered " +
      "in the database 'default'.") shouldBe true
  }

  "UDF" should "be used with udf(...) method" in {
    val udfEvenResolver = udf(evenFlagResolver _)

    val rows = letterNumbers.select($"letter", udfEvenResolver($"number") as "isEven")
      .map(row => (row.getString(0), row.getBoolean(1)))
      .collectAsList()

    rows should contain allOf(("A", false), ("B", true), ("C", false), ("D", true), ("E", false), ("F", true))
  }

  "UDF taking arguments" should "be correctly called in select expression" in {
    val udfMultiplicatorCurried = udf(multiplicatorCurried(5)_)

    val rows = letterNumbers.select($"number", udfMultiplicatorCurried($"number") as "isEven")
      .map(row => (row.getInt(0), row.getInt(1)))
      .collectAsList()

    rows should contain allOf((1,5), (2,10), (3,15), (4,20), (5,25), (6,30))
  }

  "UDF not optimized" should "slow processing down" in {
    val udfNotOptimized = udf(WebService.callWebServiceWithoutCache _)

    val watchedMoviesPerUser = Seq(
      (1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (3, 2), (3, 4), (3, 5)
    ).toDF("user", "movie")

    // Unoptimized UDF
    val startUnoptimizedProcessing = System.currentTimeMillis()
    val rowsUnoptimized = watchedMoviesPerUser.select($"user", udfNotOptimized($"movie") as "movie_title")
      .map(row => (row.getInt(0), row.getString(1)))
      .collectAsList()
    val endUnoptimizedProcessing = System.currentTimeMillis()
    val totalUnoptimizedProcessingTime = endUnoptimizedProcessing - startUnoptimizedProcessing

    // Optimized UDF
    val startOptimizedProcessing = System.currentTimeMillis()
    val udfOptimized = udf(WebService.callWebServiceWithCache _)
    val rowsOptimized = watchedMoviesPerUser.select($"user", udfOptimized($"movie") as "movie_title")
      .map(row => (row.getInt(0), row.getString(1)))
      .collectAsList()
    val endOptimizedProcessing = System.currentTimeMillis()

    val totalOptimizedProcessingTime = endOptimizedProcessing - startOptimizedProcessing

    rowsUnoptimized should contain allOf(
      (1, "Title_1"), (1, "Title_2"), (1, "Title_3"), (2, "Title_1"), (2, "Title_2"), (3, "Title_2"),
      (3, "Title_4"), (3, "Title_5")
    )
    rowsOptimized should contain allOf(
      (1, "Title_1"), (1, "Title_2"), (1, "Title_3"), (2, "Title_1"), (2, "Title_2"), (3, "Title_2"),
      (3, "Title_4"), (3, "Title_5")
    )
    totalUnoptimizedProcessingTime should be > totalOptimizedProcessingTime
  }

  "UDF" should "also be able to process 2 columns" in {
    def concatenateStrings(separator: String)(column1Value: String, column2Value: String): String = {
      s"${column1Value}${separator}${column2Value}"
    }

    val udfConcatenator = udf(concatenateStrings("-") _)
    val rows = letters.select(udfConcatenator($"lowercased_letter", $"uppercased_letter") as "word")
      .map(row => (row.getString(0)))
      .collectAsList()

    rows should contain allOf("a-A", "b-B", "c-C", "d-D", "e-E", "f-F")
  }

  "UDF" should "also be callable in nested way" in {
    def concatenateStrings(separator: String)(column1Value: String, column2Value: String): String = {
      s"${column1Value}${separator}${column2Value}"
    }

    def reverseText(text: String): String = {
      text.reverse
    }

    val udfConcatenator = udf(concatenateStrings("-") _)
    val udfReverser = udf(reverseText _)

    // Nested calls are easy to implement but from 3 functions
    // the code becomes less and less readable
    val rows = letters.select(udfReverser(udfConcatenator($"lowercased_letter", $"uppercased_letter")) as "reversed_word")
      .map(row => (row.getString(0)))
      .collectAsList()

    rows should contain allOf("A-a", "B-b", "C-c", "D-d", "E-e", "F-f")
  }

  "UDF" should "be used in where clause" in {
    val evenNumbersFilter: (Int) => Boolean = (nr) => { nr%2 == 0 }
    sparkSession.udf.register("EvenFlagResolver_whereTest", evenNumbersFilter)

    val evenNumbers = letterNumbers.selectExpr("letter", "number")
      .where("EvenFlagResolver_whereTest(number) == true")
      .map(row => (row.getString(0), row.getInt(1)))

    val rows = evenNumbers.collectAsList()

    evenNumbers.explain(true)
    rows should contain allOf(("B", 2), ("D", 4), ("F", 6))
  }

}

object WebService {

  val titresCache = mutable.Map[Int, String]()

  def callWebServiceWithoutCache(id: Int): String = {
    // Let's suppose that our function call a web service
    // to enrich row data and that the call takes 200 ms every time
    Thread.sleep(200)
    s"Title_${id}"
  }

  def callWebServiceWithCache(id: Int): String = {
    titresCache.getOrElseUpdate(id,  {
      val title = callWebServiceWithoutCache(id)
      title
    })
  }

}
