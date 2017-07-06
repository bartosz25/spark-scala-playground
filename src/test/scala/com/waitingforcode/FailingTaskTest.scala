package com.waitingforcode

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class FailingTaskTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Spark failing task test").setMaster("local[3,5]")
    .set("spark.task.maxFailures", "5")
    .set("spark.executor.extraClassPath", sys.props("java.class.path"))
  var sparkContext:SparkContext = null

  before {
    sparkContext = SparkContext.getOrCreate(conf)
  }

  after {
    sparkContext.stop
  }

  "whole job" should "fail after 5 failures of one particular task" in {
    val data = 1 to 500
    val inputRdd = sparkContext.parallelize(data, 3)

    inputRdd.map(number => {
      if (number == 250) {
        throw new RuntimeException("This exception is thrown to simulate task failures and lead to job failure")
      }
      s"Number#${number}"
    }).count()
  }

}
