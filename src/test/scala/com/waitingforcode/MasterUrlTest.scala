package com.waitingforcode

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class MasterUrlTest extends FlatSpec with Matchers with BeforeAndAfter {

  var sparkContext:SparkContext = null

  after {
    sparkContext.stop()
  }
  val receiver = new Receiver[Int](StorageLevel.MEMORY_ONLY) {
    var receiver:Thread = null
    override def onStart(): Unit = {
      // Run as new thread since it must be non-blocking
      // In the real world we would use something like
      // while(true) {// ...} that is a blocking
      // operation
      receiver = new Thread(new Runnable() {
        override def run(): Unit = {
          println("Starting thread")
          for (nr <- 0 to 4) {
            store(nr)
          }
        }
      })
      receiver.start()
    }

    override def onStop(): Unit = {
      receiver.interrupt()
    }
  }

  "local master url" should "correctly execute simple batch computation" in {
    val conf = new SparkConf().setAppName("MasterUrl local test").setMaster("local")
    sparkContext= SparkContext.getOrCreate(conf)

    val multipliedNumbers = sparkContext.parallelize(1 to 10)
      .map(_*2)
      .collect()

    multipliedNumbers should contain allOf(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)
  }

  "local master url" should "correctly execute only 1 from 2 receiver input streams" in {
    // As explained in http://spark.apache.org/docs/latest/streaming-programming-guide.html#points-to-remember-1
    // and http://www.waitingforcode.com/apache-spark-streaming/receivers-in-spark-streaming/read
    // each receiver uses 1 thread. So running 2 receivers on "local"
    // master URL will execute only 1 of them
    // Note: sometimes any of receivers will be executed because of lack
    //       of resources.
    val conf = new SparkConf().setAppName("MasterUrl local receivers test").setMaster("local")
    sparkContext= SparkContext.getOrCreate(conf)
    val streamingContext = new StreamingContext(sparkContext, Durations.seconds(2))

    val receiverDStream1 = streamingContext.receiverStream(receiver)
    val receiverDStream2 = streamingContext.receiverStream(receiver)

    val numbersAccumulator = sparkContext.collectionAccumulator[Int]("numbers-accumulator")
    receiverDStream1.union(receiverDStream2).foreachRDD(rdd => {
      rdd.foreach(numbersAccumulator.add(_))
    })

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(5000)
    streamingContext.stop()

    numbersAccumulator.value.size shouldEqual(5)
    numbersAccumulator.value should contain allOf(0, 1, 2, 3, 4)
  }

  "local[3] master url" should "correctly execute 2 receiver input streams" in {
    // Unlike previously, this time 2 threads are reserved to receivers
    // Normally the test should pass every time because
    // it follows the rule of
    // "use “local[n]” as the master URL, where n > number of receivers to run"
    // (http://spark.apache.org/docs/2.1.1/streaming-programming-guide.html#points-to-remember-1)
    val conf = new SparkConf().setAppName("MasterUrl local[3] receiver test").setMaster("local[3]")
    sparkContext= SparkContext.getOrCreate(conf)
    val streamingContext = new StreamingContext(sparkContext, Durations.seconds(2))

    val numbersAccumulator = sparkContext.collectionAccumulator[Int]("numbers-accumulator")
    val receiverDStream1 = streamingContext.receiverStream(receiver)
    val receiverDStream2 = streamingContext.receiverStream(receiver)
    receiverDStream1.union(receiverDStream2).foreachRDD(rdd => {
      rdd.foreach(numbersAccumulator.add(_))
    })

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(5000)
    streamingContext.stop()

    numbersAccumulator.value.size shouldEqual(10)
    numbersAccumulator.value should contain theSameElementsAs Iterable(0, 1, 2, 3, 4, 0, 1, 2, 3, 4)
  }

}
