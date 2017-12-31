package com.waitingforcode.heartbeat

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class SlowJobTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Spark failing task test").setMaster("spark://localhost:7077")
    .set("spark.executor.heartbeatInterval", "1s")
    .set("spark.network.timeoutInterval", "5s")
    .set("spark.network.timeout", "5s")
    .set("spark.executor.extraClassPath", sys.props("java.class.path"))
  val sparkContext:SparkContext = SparkContext.getOrCreate(conf)

  after {
    sparkContext.stop
  }

  "the job" should "fail when executors go down" in {
    val data = 1 to 500000
    val inputRdd = sparkContext.parallelize(data, 300)

    println("Before processing")
    inputRdd.map(number => {
      // just to avoid the task to finish before executors shutdown
      Thread.sleep(1000)
      s"Number#${number}"
    }).count()
  }

}