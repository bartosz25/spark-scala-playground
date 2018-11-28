package com.waitingforcode

import com.waitingforcode.util.{InMemoryLogAppender, LogMessage}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class JobTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Spark jobs test").setMaster("local")
  var sparkContext:SparkContext = null

  before {
    sparkContext = SparkContext.getOrCreate(conf)
  }

  after {
    sparkContext.stop
  }

  "2 jobs" should "be triggered where processing has 2 actions" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("Starting job"))

    val numbers = sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6))

    // The 1st processing - from here to count() action
    val pairNumbersRdd = numbers.filter(_%2 == 0)
    val pairNumbersCount = numbers.filter(_%2 == 0).count()

    // The 2nd processing - only collect() action
    // It's only for learning purposes - obviously, this code
    // is redundant since we can deduct the count from
    // collected elements
    val allPairNumbers = pairNumbersRdd.collect()

    logAppender.messages.size shouldEqual(2)
    logAppender.getMessagesText should contain allOf("Starting job: count at JobTest.scala:27",
      "Starting job: collect at JobTest.scala:33")
  }

  "different number of stages" should "be created for 1 and 2 partitions" in {
    // This time we're expecing to detect log events showing
    // new task submission
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("Adding task set"))

    val numbers = sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6))

    // The 1st test - single partition
    val numbersQuantity = numbers.count()

    // The 2nd test - 2 partitions, thus automatically
    // twice more stages
    val numbers2Partitions = sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6), 2)
    val numbersQuantityFor2Partitions = numbers2Partitions.count()

    // Expectations - the first concerns numbers.count(), the second
    // numbers2Partitions.count()
    logAppender.getMessagesText should contain allOf("Adding task set 0.0 with 1 tasks",
      "Adding task set 1.0 with 2 tasks")
  }

  "failed task" should "be sent from TaskSetManager to DAGScheduler" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("failed 1 times; aborting job", "Removed TaskSet 0.0",
      "After removal of stage"))

    val numbersRdd = sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6), 3)

    try {
      val numberOfElements = numbersRdd.filter(number => {
        if (number == 4) {
          throw new RuntimeException("4 is invalid number")
        }
        true
      }).count
    } catch {
      case _:Throwable =>
    }
    // Sleep is mandatory to catch DAGScheduler event log
    Thread.sleep(3000)

    logAppender.messages should contain allOf(
      LogMessage("Task 1 in stage 0.0 failed 1 times; aborting job", "org.apache.spark.scheduler.TaskSetManager"),
      LogMessage("Removed TaskSet 0.0, whose tasks have all completed, from pool ",
        "org.apache.spark.scheduler.TaskSchedulerImpl"),
      // For DAGScheduler other log message is more meaningful but it
      // contains execution time that can vary depending on execution environment, e.g.:
      // "ResultStage 0 (count at JobTest.scala:77) failed in 0.891 s due to Job aborted ...
      //  Driver stacktrace: (org.apache.spark.scheduler.DAGScheduler:54)"
      // It's why the test checks against the message occurring after it that is
      // stage removal.
      LogMessage("After removal of stage 0, remaining stages = 0", "org.apache.spark.scheduler.DAGScheduler")
      )
  }
}
