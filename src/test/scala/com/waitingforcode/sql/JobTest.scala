package com.waitingforcode.sql

import com.waitingforcode.util.InMemoryLogAppender
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * The goal of this test is to show that Spark SQL also uses
  * the abstraction of job, tasks and stages.
  */
class JobTest extends FlatSpec with Matchers with BeforeAndAfter {

  var sparkSession: SparkSession = null

  before {
    sparkSession = SparkSession.builder()
      .appName("Spark SQL job test").master("local[1]").getOrCreate()
  }

  after {
    sparkSession.stop()
  }

  "SQL processing" should "also be composed of job, tasks and stages" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("Starting job", "Final stage",
      "Running task 0.0"))
    val numbersDataFrame = sparkSession.range(1, 20)

    numbersDataFrame.count()

    logAppender.messages.size shouldEqual(4)
    logAppender.getMessagesText() should contain allOf("Starting job: count at JobTest.scala:29",
      "Final stage: ResultStage 1 (count at JobTest.scala:29)",
      "Running task 0.0 in stage 0.0 (TID 0)", "Running task 0.0 in stage 1.0 (TID 1)"
      )
  }


}
