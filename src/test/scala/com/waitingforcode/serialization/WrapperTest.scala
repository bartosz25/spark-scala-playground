package com.waitingforcode.serialization

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class WrapperTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Spark serialization problems test").setMaster("local")
  var sparkContext:SparkContext = null

  before {
    sparkContext = SparkContext.getOrCreate(conf)
  }

  after {
    sparkContext.stop
  }

  "serializable wrapper" should "help to create not serializable object" in {
    val labels = sparkContext.parallelize(0 to 1)
      .map(number => new SerializableWrapper(number))
      .collect()

    labels.map(wrapper => wrapper.notSerializableEntity.label) should contain
      allOf("Number#0", "Number#1")
  }

  "not serializable object without wrapper" should "make processing fail" in {
    val serializableException = intercept[SparkException] {
      sparkContext.parallelize(0 to 1)
        .map(number => new NotSerializableEntity(number))
        .collect()
    }

    serializableException.getMessage
      .contains("object not serializable (class: com.waitingforcode.serialization.NotSerializableEntity") shouldBe(true)
  }

}


class SerializableWrapper(number: Int) extends Serializable {
  // Wraps not serializable entity
  // Let's suppose that NotSerializableEntity is a 3rd part class
  // and we can't modify it
  lazy val notSerializableEntity = new NotSerializableEntity(number)
  def createEntity(number: Int): NotSerializableEntity = {
    new NotSerializableEntity(number)
  }
}

class NotSerializableEntity(number: Int) {
  val label = s"Number#${number}"
}