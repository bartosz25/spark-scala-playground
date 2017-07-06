package com.waitingforcode.action

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.reflect.io.{Directory, File}

/**
  * This test shows what happens when we use an {@code accumulator} mixed with
  * {@link RDD#isEmpty} and some other actions.
  *
  * In fact {@code isEmpty()} executes the transformations only once. Thus if the accumulator is used,
  * it can collect inconsistent results.
  */
class IsEmptyExecutingTransformationTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Double transformation execution test").setMaster("local")
  var sparkContext:SparkContext = null
  val outputDir = "/tmp/spark-isempty-numbers"

  before {
    sparkContext = SparkContext.getOrCreate(conf)
    Directory(outputDir).deleteRecursively()
  }

  after {
    sparkContext.stop
  }

  "isEmpty() method" should "invoke the transformation on 1 element" in {
    val numbersRdd = sparkContext.parallelize(Seq(1, 2, 3, 4, 5))
    val numbersAccumulator = sparkContext.collectionAccumulator[Int]("numbers accumulator")

    val numbersMultipliedBy2 = numbersRdd.map(number => {
      println(s"Mapping number ${number}")
      numbersAccumulator.add(number)
      number * 2
    })


    if (numbersMultipliedBy2.isEmpty()) {
      println("Numbers are empty")
    }

    numbersAccumulator.value.size shouldEqual(1)
    numbersAccumulator.value should contain only(1)
  }

  "isEmpty() method invoked with other action" should "make accumulated data false" in {
    val numbersRdd = sparkContext.parallelize(Seq(1, 2, 3, 4, 5))
    val numbersAccumulator = sparkContext.collectionAccumulator[Int]("numbers accumulator")

    val numbersMultipliedBy2 = numbersRdd.map(number => {
      println(s"Mapping number ${number}")
      numbersAccumulator.add(number)
      number * 2
    })

    // <!> It's an anti-pattern, written here
    // only to show possible errors of incorrect
    // implementation
    if (!numbersMultipliedBy2.isEmpty()) {
      numbersMultipliedBy2.saveAsTextFile(outputDir)
    }

    Directory(outputDir).exists should be(true)
    numbersAccumulator.value.size shouldEqual(6)
    // the "1" is duplicated because if was read twice:
    // by isEmpty() and by saveAsTextFile() actions
    numbersAccumulator.value should contain theSameElementsAs(Seq(1, 1, 2, 3, 4, 5))
  }

  "isEmpty() with other action" should "not execute accumulator twice" in {
    val numbersRdd = sparkContext.parallelize[Int](Seq(1, 2, 3, 4, 5))
    val numbersAccumulator = sparkContext.collectionAccumulator[Int]("numbers accumulator")

    val numbersMultipliedBy2 = numbersRdd.map(number => {
      println(s"Mapping number ${number}")
      numbersAccumulator.add(number)
      number * 2
    })

    numbersMultipliedBy2.cache()
    if (!numbersMultipliedBy2.isEmpty()) {
      numbersMultipliedBy2.saveAsTextFile(outputDir)
    }

    Directory(outputDir).exists should be(true)
    numbersAccumulator.value.size shouldEqual(5)
    numbersAccumulator.value should contain theSameElementsAs(Seq(1, 2, 3, 4, 5))
  }

}
