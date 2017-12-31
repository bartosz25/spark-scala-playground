package com.waitingforcode.heartbeat

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class FailedJobTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Spark failing task test").setMaster("spark://localhost:7077")
    .set("spark.executor.heartbeatInterval", "5s")
    .set("spark.network.timeoutInterval", "1s")
    .set("spark.network.timeout", "1s")
    .set("spark.executor.extraClassPath", sys.props("java.class.path"))
  val sparkContext:SparkContext = SparkContext.getOrCreate(conf)

  after {
    sparkContext.stop
  }

  "the job" should "never start if the heartbeat interval is greater than the network timeout" in {
    val data = 1 to 5
    val inputRdd = sparkContext.parallelize(data)

    val error = intercept[Exception] {
      inputRdd.map(number => {
        s"Number#${number}"
      }).count()
    }

    error.getMessage should include("Executor heartbeat timed out after")
  }

}
