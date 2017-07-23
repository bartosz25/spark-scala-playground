package com.waitingforcode

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, FunSuite, Matchers}

import scala.collection.immutable.ListMap
import scala.collection.mutable

/**
  * Micro-benchmark showing that more is not always better.
  * The test assertions correspond to the results generated on my
  * Lenovo Core i3 laptop having 4 cores. The results can vary in other environments
  */
class PartitionsNumberTest extends FlatSpec with BeforeAndAfter with Matchers {

  val conf = new SparkConf().setAppName("Spark partitions micro-benchmark").setMaster("local[*]")
  val sparkContext = new SparkContext(conf)

  after {
    sparkContext.stop()
  }

  "the optimal number of partitions" should "make processing faster than too few partitions" in {
    val resultsMap = mutable.HashMap[Int, Map[Int, Long]]()

    for (run <- 1 to 15) {
      val executionTime1Partition = executeAndMeasureFiltering(1)
      val executionTime6Partition = executeAndMeasureFiltering(6)
      val executionTime8Partition = executeAndMeasureFiltering(8)
      val executionTime25Partition = executeAndMeasureFiltering(25)
      val executionTime50Partition = executeAndMeasureFiltering(50)
      val executionTime100Partition = executeAndMeasureFiltering(100)
      val executionTime500Partition = executeAndMeasureFiltering(500)
      if (run > 5) {
        val executionTimes = Map(
          1 -> executionTime1Partition, 6 -> executionTime6Partition, 8 -> executionTime8Partition,
          25 -> executionTime25Partition, 50 -> executionTime50Partition, 100 -> executionTime100Partition,
          500 -> executionTime500Partition
        )
        resultsMap.put(run, executionTimes)
      }
    }

    val points = Seq(7, 6, 5, 4, 3, 2, 1)
    val pointsPerPartitions = mutable.HashMap(1 -> 0, 6 -> 0, 8 -> 0, 25 -> 0, 50 -> 0, 100 -> 0, 500 -> 0)
    val totalExecutionTimes = mutable.HashMap(1 -> 0L, 6 -> 0L, 8 -> 0L, 25 -> 0L, 50 -> 0L, 100 -> 0L, 500 -> 0L)
    for (resultIndex <- 6 to 15) {
      val executionTimes = resultsMap(resultIndex)
      val sortedExecutionTimes = ListMap(executionTimes.toSeq.sortBy(_._2):_*)
      val pointsIterator = points.iterator
      sortedExecutionTimes.foreach(entry => {
        val newtPoints = pointsPerPartitions(entry._1) + pointsIterator.next()
        pointsPerPartitions.put(entry._1, newtPoints)
        val newTotalExecutionTime = totalExecutionTimes(entry._1) + entry._2
        totalExecutionTimes.put(entry._1, newTotalExecutionTime)
      })
    }

    // The results for 6 and 8 partitions are similar;
    // Sometimes 6 partitions can globally perform better than 8, sometimes it's the contrary
    // It's why we only make an assumptions on other number of partitions
    pointsPerPartitions(8) should be > pointsPerPartitions(1)
    pointsPerPartitions(6) should be > pointsPerPartitions(25)
    pointsPerPartitions(8) should be > pointsPerPartitions(25)
    pointsPerPartitions(8) should be > pointsPerPartitions(50)
    pointsPerPartitions(8) should be > pointsPerPartitions(100)
    pointsPerPartitions(8) should be > pointsPerPartitions(500)
    // The assumptions below show that, unlike primitive thought we'd have, more partitions
    // doesn't necessarily mean better results. They show that the task and results distribution overheads
    // also have their importance for job's performance
    pointsPerPartitions(25) should be > pointsPerPartitions(50)
    pointsPerPartitions(50) should be > pointsPerPartitions(100)
    pointsPerPartitions(100) should be > pointsPerPartitions(500)
    // Surprisingly, the sequential computation (1 partition) is faster than the distributed and unbalanced
    // computation
    pointsPerPartitions(1) should be > pointsPerPartitions(500)
    // Below print is only for debugging purposes
    totalExecutionTimes.foreach(entry => {
      val avgExecutionTime = entry._2.toDouble/10d
      println(s"Avg execution time for ${entry._1} partition(s) = ${avgExecutionTime} ms")
    })
  }

  private def executeAndMeasureFiltering(partitionsNumber: Int): Long = {
    var executionTime = 0L
    val data = sparkContext.parallelize(1 to 10000000, partitionsNumber)

    val startTime = System.currentTimeMillis()
    val evenNumbers = data.filter(nr => nr%2 == 0)
      .count()
    val stopTime = System.currentTimeMillis()
    executionTime = stopTime - startTime
    executionTime
  }
}
