package com.waitingforcode.serialization

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class LazyLoadingTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Spark lazy loading singleton test").setMaster("local")
  var sparkContext:SparkContext = null

  before {
    sparkContext = SparkContext.getOrCreate(conf)
  }

  after {
    sparkContext.stop
  }

  "lazy loaded not serializable object" should "be correctly sent through network" in {
    val numbersAccumulator = sparkContext.collectionAccumulator[Int]("iterated numbers accumulator")
    val connector = NotSerializableLazyConnector()
    sparkContext.parallelize(0 to 1)
      .foreachPartition(numbers => {
        numbers.foreach(number => {
          connector.push(number)
          numbersAccumulator.add(number)
        })
      })

    numbersAccumulator.value should contain allOf(0, 1)
  }

  "lazy loaded not serializable object" should "be correctly sent once through network" in {
    val numbersAccumulator = sparkContext.collectionAccumulator[Int]("iterated numbers accumulator")
    // This version is a variation of the previous test because it
    // sends given object only once and thanks to that we can, for example,
    // keep the connection open for all tasks executed on given executor.
    // The connector keeps its connection "open" because it initialized lazily and since it's created from a broadcast variable,
    // it's guaranteed that only once such instance exists in the executor. 
    val connectorBroadcast = sparkContext.broadcast(NotSerializableLazyConnector())
    sparkContext.parallelize(0 to 1)
      .foreachPartition(numbers => {
        numbers.foreach(number => {
          connectorBroadcast.value.push(number)
          numbersAccumulator.add(number)
        })
      })
    sparkContext.parallelize(4 to 5)
      .foreachPartition(numbers => {
        numbers.foreach(number => {
          connectorBroadcast.value.push(number)
          numbersAccumulator.add(number)
        })
      })
    sparkContext.parallelize(7 to 8)
      .foreachPartition(numbers => {
        numbers.foreach(number => {
          connectorBroadcast.value.push(number)
          numbersAccumulator.add(number)
        })
      })

    numbersAccumulator.value should contain allOf(0, 1, 4, 5, 7, 8)
    NotSerializableLazyConnector.InitializationCount.get() shouldEqual 1
    NotSerializableLazyConnector.InitializationCount.set(0)
  }

  "eagerly loaded not serializable object" should "make processing fail" in {
    val connector = NotSerializableEagerConnector()
    val sparkException = intercept[SparkException] {
      sparkContext.parallelize(0 to 1)
        .foreachPartition(numbers => {
          numbers.foreach(number => {
            connector.push(number)
          })
        })
    }

    sparkException.getCause.getMessage
      .contains("object not serializable (class: com.waitingforcode.serialization.NotSerializableSender")
      .shouldBe(true)
  }

}

class NotSerializableEagerConnector(creator: () => NotSerializableSender) extends Serializable {

  val sender = creator()

  def push(value: Int) = {
    sender.push(value)
  }
}

object NotSerializableEagerConnector {
  def apply(): NotSerializableEagerConnector = {
    new NotSerializableEagerConnector(() => new NotSerializableSender())
  }
}

class NotSerializableLazyConnector(creator: () => NotSerializableSender) extends Serializable {

  lazy val sender = creator()

  def push(value: Int) = {
    sender.push(value)
  }
}

class NotSerializableSender {
  def push(value: Int) = {
    println(s"Pushing ${value}")
  }
}

object NotSerializableLazyConnector {
  val InitializationCount = new AtomicInteger(0)

  def apply(): NotSerializableLazyConnector = {
    InitializationCount.incrementAndGet()
    new NotSerializableLazyConnector(() => new NotSerializableSender())
  }
}

