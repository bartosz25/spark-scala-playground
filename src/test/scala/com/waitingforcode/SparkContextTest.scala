package com.waitingforcode


import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

class SparkContextTest extends FlatSpec with Matchers {

  "two SparkContexts" should "fail the processing" in {
    val exception = intercept[Exception] {
      new SparkContext("local", "SparkContext#1")
      new SparkContext("local", "SparkContext#2")
    }

    exception.getMessage should startWith("Only one SparkContext may be running in this JVM (see SPARK-2243)")
  }

  "two SparkContext created with a factory method" should "not fail" in {
    // getOrCreate is a factory method working with singletons
    val sparkContext1 = SparkContext.getOrCreate(new SparkConf().setAppName("SparkContext#1").setMaster("local"))
    val sparkContext2 = SparkContext.getOrCreate(new SparkConf().setAppName("SparkContext#2").setMaster("local"))

    sparkContext1.parallelize(Seq(1, 2, 3))
    sparkContext2.parallelize(Seq(4, 5, 6))

    sparkContext1 shouldEqual sparkContext2
  }

  "two SparkContexts created with allowMultipleContexts=true" should "work" in {
    val sparkConfiguration = new SparkConf().set("spark.driver.allowMultipleContexts", "true")
    val sparkContext1 = new SparkContext("local", "SparkContext#1", sparkConfiguration)
    val sparkContext2 = new SparkContext("local", "SparkContext#2", sparkConfiguration)
  }

}