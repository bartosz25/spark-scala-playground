package com.waitingforcode

import java.util.Date
import java.util.concurrent.ThreadLocalRandom

import org.apache.spark.rdd.RDDBarrier
import org.apache.spark.{BarrierTaskContext, SparkConf, SparkContext, SparkException}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable


class BarrierTest  extends FlatSpec with Matchers with BeforeAndAfter {

  private var sparkContext:SparkContext = null

  after {
    FailureFlags.wasFailed = false
    sparkContext.stop
    TimesContainer.IdentityMappedNumbers.clear()
  }

  behavior of "Barrier Execution Mode"

  it should "get stuck because of insufficient resources" in {
    createSparkContext("Barrier Execution Mode with insufficient resources",
      "local[1]", Map("spark.scheduler.barrier.maxConcurrentTasksCheck.maxFailures" -> "1"))
    val numbersRdd = sparkContext.parallelize(1 to 100, 3)

    val barrierException = intercept[SparkException] {
      val mappedNumbers: RDDBarrier[Int] = numbersRdd.map(number => {
        number
      }).barrier()

      mappedNumbers.mapPartitions(numbers => {
        println(s"All numbers are ${numbers.mkString(", ")}")
        Iterator("a", "b")
      }).collect()
    }

    barrierException.getMessage shouldEqual "[SPARK-24819]: Barrier execution mode does not allow run a barrier stage that requires more slots than the total number of slots in the cluster currently. Please init a new cluster with more CPU cores or repartition the input RDD(s) to reduce the number of slots required to run this barrier stage."
  }

  it should "run all tasks in parallel when there are enough resources" in {
    createSparkContext("Barrier Execution Mode with sufficient resources", "local[3]")
    val numbersRdd = sparkContext.parallelize(1 to 100, 3)

    val mappedNumbers: RDDBarrier[Int] = numbersRdd.map(number => {
      number
    }).barrier()

    val collectedNumbers = mappedNumbers.mapPartitions(numbers => {
      numbers
    }).collect()

    collectedNumbers should have size 100
    collectedNumbers should contain allElementsOf (1 to 100)
  }

  it should "restart all stages together for ShuffleMapStage" in {
    createSparkContext("Barrier Execution Mode ShuffleMapStage failure", "local[3, 100]")
    val numbersRdd = sparkContext.parallelize(1 to 10, 3)

    val mappedNumbers = numbersRdd.filter(number => {
      number % 2 == 0
    }).groupBy(number => {
      // Please notice: the retry applies only for ShuffleMapStage
      // For ResultStage it fails because:
      // Abort the failed result stage since we may have committed output for some partitions.
      if (number == 5 && !FailureFlags.wasFailed) {
        FailureFlags.wasFailed = true
        throw new SparkException("Expected failure")
      }
      number
    }).barrier()

    val collectedNumbers = mappedNumbers.mapPartitions(numbers => {
      val context = BarrierTaskContext.get()
      println(s"context=${context.taskAttemptId()} / ${context.stageAttemptNumber()}")
      context.barrier()
      numbers
    }).collect()

    collectedNumbers should have size 5
  }

  it should "not restart the tasks for result stage" in {
    createSparkContext("Barrier Execution Mode ResultStage failure", "local[3, 100]")
    val numbersRdd = sparkContext.parallelize(1 to 10, 3)

    val error = intercept[SparkException] {
      val mappedNumbers = numbersRdd.filter(number => {
        if (number == 5 && !FailureFlags.wasFailed) {
          FailureFlags.wasFailed = true
          throw new SparkException("Expected failure")
        }
        number % 2 == 0
      }).barrier()

      mappedNumbers.mapPartitions(numbers => {
        val context = BarrierTaskContext.get()
        context.barrier()
        numbers
      }).collect()
    }

    error.getMessage should include("Job aborted due to stage failure: Could not recover from a failed barrier " +
      "ResultStage. Most recent failure reason: Stage failed because barrier task ResultTask")
  }

  "RDDBarrier without BarrierTaskContext" should "synchronize tasks" in {
    createSparkContext("Barrier Execution Mode without BarrierTaskContext", "local[3]")
    val numbersRdd = sparkContext.parallelize(1 to 10, 3)

    val mappedNumbers = numbersRdd.map(number => {
      number
    }).barrier()

    mappedNumbers.mapPartitions(numbers => {
      val sleepingTime = ThreadLocalRandom.current().nextLong(1000L, 10000L)
      println(s"Sleeping ${sleepingTime} ms")
      Thread.sleep(sleepingTime)
      TimesContainer.addIdentityMappedNumbers(new Date().toString)
      numbers
    }).collect()

    TimesContainer.IdentityMappedNumbers should have size 3
    TimesContainer.IdentityMappedNumbers.distinct should have size 3
  }

  "RDDBarrier with BarrierTaskContext" should "synchronize tasks" in {
    createSparkContext("Barrier Execution Mode with BarrierTaskContext", "local[3]")
    val numbersRdd = sparkContext.parallelize(1 to 10, 3)

    val mappedNumbers = numbersRdd.map(number => {
      number
    }).barrier()

    mappedNumbers.mapPartitions(numbers => {
      val sleepingTime = ThreadLocalRandom.current().nextLong(1000L, 10000L)
      println(s"Sleeping ${sleepingTime} ms")
      Thread.sleep(sleepingTime)
      val context = BarrierTaskContext.get()
      context.barrier()
      TimesContainer.addIdentityMappedNumbers(new Date().toString)
      numbers
    }).collect()

    TimesContainer.IdentityMappedNumbers should have size 3
    TimesContainer.IdentityMappedNumbers.distinct should have size 1
  }


  private def createSparkContext(appName: String, master: String, options: Map[String, String] = Map.empty) = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    options.foreach {
      case (name, value) => conf.set(name, value)
    }
    sparkContext = SparkContext.getOrCreate(conf)
  }

}

object FailureFlags {
  var wasFailed = false
}

object TimesContainer {
  val IdentityMappedNumbers = new mutable.ListBuffer[String]()
  def addIdentityMappedNumbers(time: String) = {
    IdentityMappedNumbers.synchronized {
      IdentityMappedNumbers.append(time)
    }
  }
}