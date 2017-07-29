package com.waitingforcode.sql

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class UserDefinedFunctionChainOfResponsibilityContainerTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val sparkSession = SparkSession.builder().appName("UDF test")
    .master("local[*]").getOrCreate()

  override def afterAll {
    sparkSession.stop()
  }


  "chain of responsibility" should "be called as a container of UDFs" in {
    val chainedFunctions = Seq(MultiplicationHandler(100), DivisionHandler(100), ExponentiationHandler(2),
      SubstractionHandler(1))
    sparkSession.udf.register("chain_of_responsibility_udf", new ChainOfResponsibilityContainer(chainedFunctions).execute _)

    import sparkSession.implicits._
    val letterNumbers = Seq(
      ("A", 50), ("B", 55), ("C", 60), ("D", 65), ("E", 70), ("F", 75)
    ).toDF("letter", "number")
    val rows = letterNumbers.selectExpr("letter", "chain_of_responsibility_udf(number) as processedNumber")
      .map(row => (row.getString(0), row.getInt(1)))
      .collectAsList()

    rows should contain allOf(("A", 2499), ("B", 3024), ("C", 3599), ("D", 4224), ("E", 4899), ("F", 5624))
  }

}


abstract class Handler() {

  private var nextHandler: Option[Handler] = None

  def setNextHandler(nextHandler: Handler): Unit = {
    this.nextHandler = Option(nextHandler)
  }

  def execute(number: Int): Int = {
    val result = executeInternal(number)
    if (nextHandler.isDefined) {
      nextHandler.get.execute(result)
    } else {
      result
    }
  }

  def executeInternal(number: Int): Int

}

case class MultiplicationHandler(factor: Int) extends Handler {
  override def executeInternal(number: Int): Int = {
    factor * number
  }
}

case class SubstractionHandler(subtrahend: Int) extends Handler {
  override def executeInternal(number: Int): Int = {
    number - subtrahend
  }
}

case class ExponentiationHandler(exponent: Int) extends Handler {
  override def executeInternal(number: Int): Int = {
    scala.math.pow(number, exponent).toInt
  }
}

case class DivisionHandler(divisor: Int) extends Handler {
  override def executeInternal(number: Int): Int = {
    number/divisor
  }
}

class ChainOfResponsibilityContainer(functions: Seq[Handler]) {
  assert(functions.nonEmpty, "There should be at least 1 UDF passed to the chain of responsibility")
  buildTheChain()

  def execute(number: Int): Int = {
    functions.head.execute(number)
  }

  private def buildTheChain(): Unit = {
    var index = 0
    while (index+1 < functions.size) {
      val currentFunction = functions(index)
      index += 1
      val nextFunction = functions(index)
      currentFunction.setNextHandler(nextFunction)
    }
  }

}