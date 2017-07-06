package com.waitingforcode.action

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}


  class TreeAggregationTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Tree aggregation test").setMaster("local")
  var sparkContext:SparkContext = null

  before {
    sparkContext = SparkContext.getOrCreate(conf)
  }

  after {
    sparkContext.stop
  }

  "treeReduce" should "compute sum of numbers" in {
    val numbersRdd = sparkContext.parallelize(1 to 20, 10)
    val sumComputation = (v1: Int, v2: Int) => v1 + v2

    val treeSum = numbersRdd.treeReduce(sumComputation, 2)

    treeSum shouldEqual(210)
    val reducedSum = numbersRdd.reduce(sumComputation)
    reducedSum shouldEqual(treeSum)
  }

  "treeAggregate" should "compute sum of numbers" in {
    val numbersRdd = sparkContext.parallelize(1 to 20, 10)
    val sumComputation = (v1: Int, v2: Int) => v1 + v2

    val treeSum = numbersRdd.treeAggregate(0)(sumComputation, sumComputation, 2)

    treeSum shouldEqual(210)
    val aggregatedSum = numbersRdd.aggregate(0)(sumComputation, sumComputation)
    aggregatedSum shouldEqual(treeSum)
  }

}
