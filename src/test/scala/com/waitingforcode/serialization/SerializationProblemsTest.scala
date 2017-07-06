package com.waitingforcode.serialization

import java.io.NotSerializableException

import org.apache.spark.sql.AnalysisException
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class SerializationProblemsTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Spark serialization problems test").setMaster("local")
  var sparkContext:SparkContext = null

  before {
    sparkContext = SparkContext.getOrCreate(conf)
  }

  after {
    sparkContext.stop
  }

  "serialization" should "fail because of not serializable mapped object" in {
    val serializableException = intercept[SparkException] {
      sparkContext.parallelize(0 to 4)
        .map(new NotSerializableNumber(_))
        .collect()
    }

    serializableException.getMessage
      .contains("object not serializable (class: com.waitingforcode.serialization.NotSerializableNumber") shouldBe(true)
  }

  "serialization" should "fail because of transformation using not serializable object" in {
    val serializableException = intercept[SparkException] {
      val numberFilter = new NotSerializableFilter(2)
      sparkContext.parallelize(0 to 4)
        .filter(numberFilter.filter(_))
        .collect()
    }

    serializableException.getCause.getMessage
      .contains("object not serializable (class: com.waitingforcode.serialization.NotSerializableFilter") shouldBe(true)
  }

  "serialization" should "fail because of not serializable broadcast" in {
    val serializableException = intercept[NotSerializableException] {
      sparkContext.broadcast(new NotSerializableBroadcast)
      sparkContext.parallelize(0 to 4).collect()
    }

    serializableException.getMessage
      .contains("object not serializable (class: com.waitingforcode.serialization.NotSerializableBroadcast") shouldBe(true)
  }

}

class NotSerializableBroadcast {}

class NotSerializableFilter(bound: Int) {
  def filter(number: Int): Boolean = {
    number > bound
  }
}

class NotSerializableNumber(number: Int) {
  val label = s"Number#${number}"
}
