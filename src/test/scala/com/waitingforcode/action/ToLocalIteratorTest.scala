package com.waitingforcode.action

import com.waitingforcode.util.InMemoryLogAppender
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class ToLocalIteratorTest extends FlatSpec with BeforeAndAfter with Matchers {

  var sparkContext: SparkContext = null
  val ExpectedLogPatterns = Seq("Finished task 0.0 in stage 0.0 (TID 0) in ",
    "Finished task 1.0 in stage 0.0 (TID 1) in ",
    "Finished task 2.0 in stage 0.0 (TID 2) in ", "Finished task 3.0 in stage 0.0 (TID 3) in ",
    "Finished task 4.0 in stage 0.0 (TID 4) in ")

  before {
    val conf = new SparkConf().setAppName("Spark toLocalIterator test").setMaster("local")
    sparkContext = new SparkContext(conf)
  }
  after {
    sparkContext.stop()
  }

  "only one task" should "be executed when the first 5 items must be retrieved" in {
    val inMemoryLogAppender = InMemoryLogAppender.createLogAppender(ExpectedLogPatterns)
    val numbersRdd = sparkContext.parallelize(1 to 100, 5)
    val numbersRddLocalIterator = numbersRdd.map(number => number * 2)
      .toLocalIterator

    // This filter could be implemented in .filter() method
    // But used as here helps to show the difference between
    // toLocalIterator and collect
    var canRun = true
    while (numbersRddLocalIterator.hasNext && canRun) {
      val partitionNumber = numbersRddLocalIterator.next()
      if (partitionNumber == 10) {
        canRun = false
      }
    }

    inMemoryLogAppender.messages.size shouldEqual (1)
    val logMessages = inMemoryLogAppender.getMessagesText()
    val taskExecution = logMessages.filter(msg => msg.startsWith(s"Finished task 0.0 ")).size
    taskExecution shouldEqual(1)
  }

  "collect invocation" should "move all data to the driver" in {
    val inMemoryLogAppender = InMemoryLogAppender.createLogAppender (ExpectedLogPatterns)
    val numbersRdd = sparkContext.parallelize (1 to 100, 5)

    val collectedNumbers = numbersRdd.map (number => number * 2).collect

    inMemoryLogAppender.messages.size shouldEqual(5)
    val logMessages = inMemoryLogAppender.getMessagesText ()
    for (i <- 0 to 4) {
      val taskExecution = logMessages.filter (msg => msg.startsWith (s"Finished task ${i}.0")).size
      taskExecution shouldEqual(1)
    }
  }
}
