package com.waitingforcode.test

import org.apache.spark.Partitioner
import org.scalatest.{FlatSpec, Matchers}


/**
  * Examples of some unit tests that can be written in Spark
  * thanks to clear separation of code without the need of
  * initializing {@link SparkContext}.
  */
class UnitTestExampleTest extends FlatSpec with Matchers {

  "filters" should "be testable without Spark context" in {
    // example of use:
    // rdd.filters(isGreaterThan(_, 5))
    Filters.isGreaterThan(1, 5) shouldBe(false)
  }

  "mapper" should "be testable without Spark context" in {
    // example of use:
    // rdd.map(mapToString(_))
    Mappers.mapToString(1) shouldEqual("Number 1")
  }

  "to pair mapper" should "be testable without Spark context" in {
    // example of use:
    // rdd.mapToPair(mapToPair(_))
    Mappers.mapToPair("vegetables: potato, carrot") shouldEqual(("vegetables", "potato, carrot"))
  }

  "partitioner" should "return 0 partition for pair key" in {
    // example of use:
    // rdd.partitionBy(new SamplePartitioner())
    val partitioner = new SamplePartitioner()

    partitioner.getPartition(4) shouldEqual(0)
  }

  "aggregate by partition" should "be testable without SparkContext" in {
    // example of use:
    // rdd.aggregate("")((text, number) => text + ";" + number, (text1, text2) => text1 + "-"+text2)

    Reducers.partitionConcatenator("123", 4) shouldEqual("123;4")
  }

  "aggregate combiner" should "be testable without SparkContext" in {
    // refer to "aggregate by partition" should "be testable without SparkContext" in
    // to see the example of use

    Reducers.combineConcatenations("123", "456") shouldEqual("123-456")
  }

}


object Filters {

  def isGreaterThan(number: Int, lowerBound: Int): Boolean = {
    number > lowerBound
  }

}

object Mappers {

  def mapToString(number: Int): String = {
    s"Number ${number}"
  }

  // mapToPair
  def mapToPair(shoppingList: String): (String, String) = {
    val textParts = shoppingList.split(": ")
    (textParts(0).trim, textParts(1).trim)
  }

}

object Reducers {

  def partitionConcatenator(concatenatedText: String, newNumber: Int): String = {
    concatenatedText + ";" + newNumber
  }

  def combineConcatenations(concatenatedText1: String, concatenatedText2: String): String = {
    concatenatedText1 + "-" + concatenatedText2
  }


}

class SamplePartitioner extends Partitioner{
  override def numPartitions: Int = 2

  override def getPartition(key: Any): Int = {
    key match {
      case nr: Int => partitionIntKey(nr)
      case _ => throw new IllegalArgumentException(s"Unsupported key ${key}")
    }
  }

  private def partitionIntKey(key: Int): Int = {
    if (key%2 == 0) 0 else 1
  }
}