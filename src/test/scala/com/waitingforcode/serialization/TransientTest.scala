package com.waitingforcode.serialization

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class TransientTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Spark lazy loading singleton test").setMaster("local")
  var sparkContext:SparkContext = null

  before {
    sparkContext = SparkContext.getOrCreate(conf)
  }

  after {
    sparkContext.stop
  }

  "transient and lazy loaded not serializable field" should "be correctly sent through network" in {
    val holderWithTransient = new HolderWithTransient
    val transientObjectAccumulator = sparkContext.collectionAccumulator[String]("accumulator of lazy transient object")
    sparkContext.parallelize(0 to 1)
      .foreachPartition(numbers => {
        numbers.foreach(number => {
          val className = holderWithTransient.notSerializableField.getClass.getName
          transientObjectAccumulator.add(className)
        })
      })

    transientObjectAccumulator.value.size() shouldEqual(2)
    transientObjectAccumulator.value.get(0) shouldEqual("com.waitingforcode.serialization.NotSerializableField")
    transientObjectAccumulator.value.get(1) shouldEqual("com.waitingforcode.serialization.NotSerializableField")
  }

  "transient and not lazy loaded not serializable field" should "be correctly sent through network as null" in {
    val holderWithNotLazyTransient = new HolderWithNotLazyTransient
    val transientObjectAccumulator = sparkContext.collectionAccumulator[String]("accumulator of lazy transient object")
    // When @transient field is not marked as lazy loaded it means that
    // it's not serialized and that's all; i.e. it's not
    // recalculated
    sparkContext.parallelize(0 to 1)
      .foreachPartition(numbers => {
        numbers.foreach(number => {
          if (holderWithNotLazyTransient.notSerializableField == null) {
            transientObjectAccumulator.add("null")
          }
        })
      })

    transientObjectAccumulator.value.size() shouldEqual(2)
    transientObjectAccumulator.value.get(0) shouldEqual("null")
    transientObjectAccumulator.value.get(1) shouldEqual("null")
  }

  "not transient not serializable field" should "make processing fail" in {
    val holderWithoutTransient = new HolderWithoutTransient
    val sparkException = intercept[SparkException] {
      sparkContext.parallelize(0 to 1)
        .foreachPartition(numbers => {
          numbers.foreach(number => {
            val field = holderWithoutTransient.notSerializableField
            println(s"Field was ${field}")
          })
        })
    }

    sparkException.getCause.getMessage
      .contains("object not serializable (class: com.waitingforcode.serialization.NotSerializableField")
      .shouldBe(true)
  }

}

class HolderWithoutTransient() extends Serializable {
  val notSerializableField = new NotSerializableField
}

class HolderWithNotLazyTransient() extends Serializable {

  @transient val notSerializableField = new NotSerializableField

}

class HolderWithTransient() extends Serializable {

  @transient lazy val notSerializableField = new NotSerializableField

}

class NotSerializableField {}
