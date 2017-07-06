package com.waitingforcode

import java.util.stream.Collectors

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable

class SingletonTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Spark singleton test")
    .setMaster("local[*]")
  var streamingContext: StreamingContext = null
  val dataQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

  before {
    streamingContext = new StreamingContext(conf, Durations.seconds(1))
  }

  after {
    streamingContext.stop(stopSparkContext = true, stopGracefully = true)
  }

  "new instance" should "be created every time in stage" in {
    for (i <- 1 to 10) {
      dataQueue += streamingContext.sparkContext.makeRDD(Seq(i), 1)
    }
    val instanceClass = new InstanceClass()
    val seenInstancesAccumulator = streamingContext.sparkContext.collectionAccumulator[Int]("seen instances")
    val queueStream = streamingContext.queueStream(dataQueue, true)
    queueStream.foreachRDD((rdd, time) => {
      rdd.foreachPartition(numbers => {
        seenInstancesAccumulator.add(instanceClass.hashCode())
        println(s"Instance of ${instanceClass} with hash code ${instanceClass.hashCode()}")
      })
    })

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(10000)

    val seenClassesHashes = seenInstancesAccumulator.value.stream().collect(Collectors.toSet())
    seenClassesHashes.size() should be > 1
  }

  "a single instance" should "be kept in every stage" in {
    for (i <- 1 to 10) {
      dataQueue += streamingContext.sparkContext.makeRDD(Seq(i), 1)
    }
    val instanceClass = SingletonObject
    val seenInstancesAccumulator = streamingContext.sparkContext.collectionAccumulator[Int]("seen instances")
    val queueStream = streamingContext.queueStream(dataQueue, true)
    queueStream.foreachRDD((rdd, time) => {
      rdd.foreachPartition(numbers => {
        seenInstancesAccumulator.add(instanceClass.hashCode())
        println(s"Instance of ${instanceClass} with hash code ${instanceClass.hashCode()}")
      })
    })

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(10000)

    val seenClassesHashes = seenInstancesAccumulator.value.stream().collect(Collectors.toSet())
    seenClassesHashes.size() shouldEqual(1)
  }

  "a single instance coming from pool of singletons" should "be kept in every stage" in {
    for (i <- 1 to 10) {
      dataQueue += streamingContext.sparkContext.makeRDD(Seq(i), 1)
    }
    val seenInstancesAccumulator = streamingContext.sparkContext.collectionAccumulator[Int]("seen instances")
    val queueStream = streamingContext.queueStream(dataQueue, true)
    queueStream.foreachRDD((rdd, time) => {
      rdd.foreachPartition(numbers => {
        val lazyLoadedInstanceClass = LazyLoadedInstanceClass.getInstance(1)
        seenInstancesAccumulator.add(lazyLoadedInstanceClass.hashCode())
        println(s"Instance of ${lazyLoadedInstanceClass} with hash code ${lazyLoadedInstanceClass.hashCode()}")
      })
    })

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(10000)

    val seenClassesHashes = seenInstancesAccumulator.value.stream().collect(Collectors.toSet())
    seenClassesHashes.size() shouldEqual(1)
  }

  "a single instance coming from class with equality logic implemented" should "be kept in every stage" in {
    for (i <- 1 to 10) {
      dataQueue += streamingContext.sparkContext.makeRDD(Seq(i), 1)
    }
    val seenInstancesAccumulator = streamingContext.sparkContext.collectionAccumulator[Int]("seen instances")
    val queueStream = streamingContext.queueStream(dataQueue, true)
    val instanceClass = new InstanceClassWithEquality(1)
    queueStream.foreachRDD((rdd, time) => {
      rdd.foreachPartition(numbers => {
        seenInstancesAccumulator.add(instanceClass.hashCode())
        println(s"Instance of ${instanceClass} with hash code ${instanceClass.hashCode()}")
      })
    })

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(10000)

    val seenClassesHashes = seenInstancesAccumulator.value.stream().collect(Collectors.toSet())
    seenClassesHashes.size() shouldEqual(1)
  }

  // The same thing as in above test can be achieved easier
  // thanks to Scala's case classes
  "a single instance coming from case class" should "be kept in every stage" in {
    for (i <- 1 to 10) {
      dataQueue += streamingContext.sparkContext.makeRDD(Seq(i), 1)
    }
    val seenInstancesAccumulator = streamingContext.sparkContext.collectionAccumulator[Int]("seen instances")
    val queueStream = streamingContext.queueStream(dataQueue, true)
    val instanceClass = InstanceClassAsCaseClass(1)
    queueStream.foreachRDD((rdd, time) => {
      rdd.foreachPartition(numbers => {
        seenInstancesAccumulator.add(instanceClass.hashCode())
        println(s"Instance of ${instanceClass} with hash code ${instanceClass.hashCode()}")
      })
    })

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(10000)

    val seenClassesHashes = seenInstancesAccumulator.value.stream().collect(Collectors.toSet())
    seenClassesHashes.size() shouldEqual(1)
  }

}

class InstanceClass extends Serializable {}

object SingletonObject extends Serializable  {}

class LazyLoadedInstanceClass(val id:Int) extends Serializable {}

object LazyLoadedInstanceClass extends Serializable {

  private val InstancesMap = mutable.Map[Int, LazyLoadedInstanceClass]()

  def getInstance(id: Int): LazyLoadedInstanceClass = {
    InstancesMap.getOrElseUpdate(id, new LazyLoadedInstanceClass(id))
  }

}

class InstanceClassWithEquality(val id:Int) extends Serializable {
  override def equals(comparedObject: scala.Any): Boolean = {
    if (comparedObject.isInstanceOf[InstanceClassWithEquality]) {
      val comparedInstance = comparedObject.asInstanceOf[InstanceClassWithEquality]
      id == comparedInstance.id
    } else {
      false
    }
  }

  override def hashCode(): Int = {
    id
  }
}

case class InstanceClassAsCaseClass(id: Int)