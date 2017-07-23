package com.waitingforcode.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class DoubleTransformationExecution extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Double transformation execution test").setMaster("local")
  var sparkContext:SparkContext = null

  before {
    sparkContext = SparkContext.getOrCreate(conf)
  }

  after {
    sparkContext.stop
  }

  "transformations" should "be executed twice since 2 actions are triggered" in {
    val rddOfNumbers = sparkContext.parallelize(Seq(1, 2, 3, 4, 5))
    val originalNumbersAccumulator = sparkContext.collectionAccumulator[Int]("original-numbers-accumulator")

    val filteredAndMultipliedRdd = rddOfNumbers.filter(number => number > 0)
      .map(number => {
        originalNumbersAccumulator.add(number)
        number * 2
      })

    // Now we call 2 actions - as expected, each of them will execute transformations
    // defined above
    val numberOfElements = filteredAndMultipliedRdd.count()
    if (numberOfElements > 0) {
      val transformedNumbers = filteredAndMultipliedRdd.collect()
    }

    originalNumbersAccumulator.value should contain theSameElementsAs Seq(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
    originalNumbersAccumulator.value.size() shouldEqual(10)
  }

  "2 action calls" should "not make transformations execute twice because of RDD caching" in {
    val rddOfNumbers = sparkContext.parallelize(Seq(1, 2, 3, 4, 5))
    val originalNumbersAccumulator = sparkContext.collectionAccumulator[Int]("original-numbers-accumulator")

    val filteredAndMultipliedRdd = rddOfNumbers.filter(number => number > 0)
      .map(number => {
        originalNumbersAccumulator.add(number)
        number * 2
      })

    // Now we call 2 actions - but unlike in previous example, in this one
    // transformations will execute only once because of caching
    filteredAndMultipliedRdd.cache()
    val numberOfElements = filteredAndMultipliedRdd.count()
    if (numberOfElements > 0) {
      val transformedNumbers = filteredAndMultipliedRdd.collect()
    }

    originalNumbersAccumulator.value should contain theSameElementsAs Seq(1, 2, 3, 4, 5)
    originalNumbersAccumulator.value.size() shouldEqual(5)
  }


}
