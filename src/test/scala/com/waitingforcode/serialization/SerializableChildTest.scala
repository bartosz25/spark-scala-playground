package com.waitingforcode.serialization

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class SerializableChildTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Spark serialization problems test").setMaster("local")
  var sparkContext:SparkContext = null

  before {
    sparkContext = SparkContext.getOrCreate(conf)
  }

  after {
    sparkContext.stop
  }

  "serializable child of not serializable parent" should "be correctly sent through network" in {
    val labels = sparkContext.parallelize(0 to 1)
      .map(number => new NotSerializableTextWrapper(number))
      .collect()

    labels.map(wrapper => wrapper.getLabel()) should contain allOf("overridden_number#0", "overridden_number#1")
  }

  "not serializable object" should "make processing fail" in {
    val serializableException = intercept[SparkException] {
      sparkContext.parallelize(0 to 1)
        .map(number => new NotSerializableText(number))
        .collect()
    }

    serializableException.getMessage
      .contains("object not serializable (class: com.waitingforcode.serialization.NotSerializableText") shouldBe(true)
  }

}

class NotSerializableTextWrapper(number: Int) extends NotSerializableText(number) with Serializable {

  override def getLabel(): String = {
    s"overridden_number#${number}"
  }


}

class NotSerializableText(number:Int) {

  // no-arg constructor is mandatory for the first
  // non serializable superclass is serializable
  def this() = {
    this(0)
  }

  def getLabel(): String = {
    s"number#${number}"
  }

}
