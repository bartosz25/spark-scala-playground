package com.waitingforcode

import java.io._
import java.util.stream.Collectors

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable

class BroadcastTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Spark broadcast test")
    .setMaster("local[*]")
  var streamingContext: StreamingContext = null
  val dataQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

  before {
    streamingContext = new StreamingContext(conf, Durations.milliseconds(500))
  }

  after {
    streamingContext.stop(stopSparkContext = true, stopGracefully = true)
  }

  "dictionary map" should "be broadcasted to executors" in {
    for (i <- 1 to 10) {
      dataQueue += streamingContext.sparkContext.makeRDD(Seq(i), 1)
    }
    val seenInstancesAccumulator = streamingContext.sparkContext.collectionAccumulator[Int]("seen instances")
    val queueStream = streamingContext.queueStream(dataQueue, true)
    val dictionary = Map("A" -> 1, "B" -> 2, "C" -> 3, "D" -> 4)
    val broadcastedDictionary = streamingContext.sparkContext.broadcast(dictionary)

    queueStream.foreachRDD((rdd, time) => {
      val dictionaryFromBroadcast = broadcastedDictionary.value
      seenInstancesAccumulator.add(dictionaryFromBroadcast.hashCode)
      rdd.foreachPartition(data => {
        seenInstancesAccumulator.add(dictionaryFromBroadcast.hashCode)
      })
    })

    streamingContext.start()
    //streamingContext.awaitTermination()
    streamingContext.awaitTerminationOrTimeout(5000)

    val seenClassesHashes = seenInstancesAccumulator.value.stream().collect(Collectors.toSet())
    seenClassesHashes.size() shouldEqual(1)
  }

  "class instance" should "be broadcaster but different instances should be found in different actions" in {
    for (i <- 1 to 10) {
      dataQueue += streamingContext.sparkContext.makeRDD(Seq(i), 1)
    }
    val seenInstancesAccumulator = streamingContext.sparkContext.collectionAccumulator[Int]("seen instances")
    val queueStream = streamingContext.queueStream(dataQueue, true)
    val notEqualityObject = new NoEqualityImplementedObject(3)
    val broadcatedObjectWithoutEquality = streamingContext.sparkContext.broadcast(notEqualityObject)

    queueStream.foreachRDD((rdd, time) => {
      val objectWithoutEquality = broadcatedObjectWithoutEquality.value
      seenInstancesAccumulator.add(objectWithoutEquality.hashCode)
      rdd.foreachPartition(data => {
        seenInstancesAccumulator.add(objectWithoutEquality.hashCode)
      })
    })

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(5000)

    val seenClassesHashes = seenInstancesAccumulator.value.stream().collect(Collectors.toSet())
    seenClassesHashes.size() should be > 1
  }

}

class NoEqualityImplementedObject(val someIdentifier: Int) extends Serializable {}