package com.waitingforcode.tips

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class RemoveDuplicatesTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL dropDuplicates tip").master("local[*]").getOrCreate()

  override def afterAll {
    sparkSession.stop()
  }

  "duplicated primitive types" should "be removed" in {
    import sparkSession.implicits._
    val letters = Seq("A", "A", "B", "C", "D", "A", "D", "O", "J").toDF("letter")


    val distinctLetters = letters.dropDuplicates().map(row => row.getAs[String]("letter")).collectAsList()

    distinctLetters should contain allOf("A", "B", "C", "D", "J", "O")
    distinctLetters should have length 6
  }

  "duplicated composite types" should "be removed" in {
    import sparkSession.implicits._
    val letters = Seq(("A", 1, true), ("A", 2, false), ("B", 3, true), ("A", 4, true))
      .toDF("letter", "number", "was_read")

    val distinctLetters = letters.dropDuplicates("letter", "was_read")
      .map(row => (row.getAs[String]("letter"), row.getAs[Int]("number"), row.getAs[Boolean]("was_read")))
      .collectAsList()

    distinctLetters should contain allOf(("A", 1, true), ("A", 2, false), ("B", 3, true))
    distinctLetters should have length 3
  }
}
