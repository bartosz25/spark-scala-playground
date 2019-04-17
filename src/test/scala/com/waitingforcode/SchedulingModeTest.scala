package com.waitingforcode

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.waitingforcode.util.{InMemoryLogAppender, LogMessage}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class SchedulingModeTest extends FlatSpec with BeforeAndAfter with Matchers {

  "FIFO scheduling mode" should "run the tasks of one job in sequential manner" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("Running task"),
      (event) => LogMessage(s"${event.getTimeStamp}; ${event.getMessage}", event.getLoggerName))
    val conf = new SparkConf().setAppName("Spark FIFO scheduling modes").setMaster("local[2]")
    conf.set("spark.scheduler.mode", "FIFO")
    val sparkContext = new SparkContext(conf)

    val latch = new CountDownLatch(2)
    new Thread(new Runnable() {
      override def run(): Unit = {
        val input1 = sparkContext.parallelize(1 to 1000000, 5)
        val evenNumbers = input1.filter(nr => {
          val isEven = nr % 2 == 0
          if (nr == 180000) {
            Thread.sleep(3000L)
          }
          isEven
        })
        evenNumbers.count()
        latch.countDown()
      }
    }).start()
    Thread.sleep(100L)
    new Thread(new Runnable() {
      override def run(): Unit = {
        val input2 = sparkContext.parallelize(1000000 to 2000000, 5)
        val oddNumbers = input2.filter(nr => {
          val isOdd = nr % 1000000 != 0
          if (nr == 1050000) {
            Thread.sleep(3000L)
          }
          isOdd
        })
        oddNumbers.count()
        latch.countDown()
      }
    }).start()

    latch.await(5, TimeUnit.MINUTES)
    sparkContext.stop()

    val timeOrderedStages = getSortedStages(logAppender.getMessagesText())
    val notSeqentualStages = timeOrderedStages.sliding(3).find(stages => {
      val allDifferentStages = stages.sliding(2).forall {
        case Seq(stage1, stage2) => stage1 != stage2
      }
      allDifferentStages
    })
    notSeqentualStages shouldBe empty
    // It's empty because the tasks stages are always submitted in sequential order. Locally, the result I had
    // the most often was:
    // ListBuffer(0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    // (but of course it will depend on the threads starting order)
    print(s"${timeOrderedStages}")
  }

  "FAIR scheduling mode" should "run stages from 2 jobs in round-robin manner" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("Running task"),
      (event) => LogMessage(s"${event.getTimeStamp}; ${event.getMessage}", event.getLoggerName))
    val conf = new SparkConf().setAppName("Spark FAIR scheduling modes").setMaster("local[2]")
    conf.set("spark.scheduler.mode", "FAIR")
    val sparkContext = new SparkContext(conf)

    val latch = new CountDownLatch(2)
    new Thread(new Runnable() {
      override def run(): Unit = {
        sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
        val input1 = sparkContext.parallelize(1 to 1000000, 5)
        val evenNumbers = input1.filter(nr => {
          val isEven = nr % 2 == 0
          if (nr == 180000) {
            Thread.sleep(3000L)
          }
          isEven
        })
        evenNumbers.count()
        latch.countDown()
      }
    }).start()
    new Thread(new Runnable() {
      override def run(): Unit = {
        sparkContext.setLocalProperty("spark.scheduler.pool", "pool12")
        val input2 = sparkContext.parallelize(1000000 to 2000000, 5)
        val oddNumbers = input2.filter(nr => {
          val isOdd = nr % 1000000 != 0
          if (nr == 1050000) {
            Thread.sleep(3000L)
          }
          isOdd
        })
        oddNumbers.count()
        latch.countDown()
      }
    }).start()

    latch.await(5, TimeUnit.MINUTES)
    sparkContext.stop()

    val timeOrderedStages = getSortedStages(logAppender.getMessagesText())
    val notSeqentualStages = timeOrderedStages.sliding(3).find(stages => {
      val allDifferentStages = stages.sliding(2).forall {
        case Seq(stage1, stage2) => stage1 != stage2
      }
      allDifferentStages
    })
    notSeqentualStages shouldBe defined
    // It will depend on the thread starting time but locally the most often I had the
    // Some(ListBuffer("0.0", "1.0", "0.0")) result
    println(s"${notSeqentualStages}")
  }

  private def getSortedStages(loggedMessages: Seq[String]): Seq[String] = {
    loggedMessages.map(timestampWithMessage => timestampWithMessage.split(";"))
      .sortBy(timestampAndMessage => timestampAndMessage(0))
      .map(timestampAndMessage => {
        val stageWithTID = timestampAndMessage(1).takeRight(12).trim
        val stageId = stageWithTID.split(" ")(0)
        stageId
      })
  }

}


