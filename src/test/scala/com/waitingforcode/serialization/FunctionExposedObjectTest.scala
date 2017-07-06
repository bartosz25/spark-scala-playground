package com.waitingforcode.serialization

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class FunctionExposedObjectTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Spark lazy loading singleton test").setMaster("local")
  var sparkContext:SparkContext = null

  before {
    sparkContext = SparkContext.getOrCreate(conf)
  }

  after {
    sparkContext.stop
  }

  "not serializable object" should "be correctly send through network" in {
    object Exposer {
      def get(): LabelMaker = {
        new LabelMaker()
      }
    }

    val labels = sparkContext.parallelize(0 to 1)
      .map(number => Exposer.get().convertToLabel(number))
      .collect()

    labels should contain allOf("Number#0", "Number#1")
  }

  "not serializable object" should "make mapping fail" in {
    val labelMaker = new LabelMaker
    val sparkException = intercept[SparkException] {
      sparkContext.parallelize(0 to 1)
        .map(number => labelMaker.convertToLabel(number))
        .collect()
    }

    sparkException.getCause.getMessage
      .contains("object not serializable (class: com.waitingforcode.serialization.LabelMaker") shouldBe(true)
  }

}

class LabelMaker() {

  def convertToLabel(number: Int): String = {
    s"Number#${number}"
  }
}
