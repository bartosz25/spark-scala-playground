package com.waitingforcode

import com.waitingforcode.util.InMemoryLogAppender
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * The goal of below tests is to prove that checking where code is executed (driver or worker) is not
  * obvious every time.
  * The tests operate on not serializable object that should file during sending to worker node.
  */
class SparkExecutionTest extends FlatSpec with Matchers with BeforeAndAfter  {

  val conf = new SparkConf().setAppName("Spark execution test").setMaster("local")
  var sparkContext:SparkContext = null

  before {
    sparkContext = SparkContext.getOrCreate(conf)
  }

  after {
    sparkContext.stop
  }

  "not serializable object executed on executor" should "make processing fail" in {
    val data = Array(1, 2, 3, 4, 5)

    val numbersRdd = sparkContext.parallelize(data)
    val multiplicator = new NotSerializableMultiplicator(5)


    val serializableException = intercept[SparkException] {
      numbersRdd.map(number => multiplicator.multiply(number))
    }
    serializableException.getMessage should include("Task not serializable")
    serializableException.getCause.getMessage should
      include("object not serializable (class: com.waitingforcode.NotSerializableMultiplicator")
  }

  "not serializable object executed in action on worker side" should "make processing fail" in {
    val data = Array(1, 2, 3, 4, 5)

    val numbersRdd = sparkContext.parallelize(data)

    val multiplicator = new NotSerializableMultiplicator(5)
    // This time we could think that multiplicator object is used on driver part,  since it manipulates
    // RDDs. But in fact it's also driver side

    // Below code is executed in action and we should think that it's done on driver's side.
    // But it's not the case - RDDs are distributed on different executors, thus
    // instead of bringing all data to driver and iterate on them, the processing
    // is applied directly on executor's side
    val serializableException = intercept[SparkException] {
      numbersRdd.foreach(number => {
        val multiplicationResult = multiplicator.multiply(number)
      })
    }
    serializableException.getMessage should include("Task not serializable")
    serializableException.getCause.getMessage should
      include("object not serializable (class: com.waitingforcode.NotSerializableMultiplicator")
  }

  "reduce operation" should "be executed partially on executor and on driver" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("Starting job", "Final stage:", "Job 0 finished"))
    val data = Array(1, 2, 3, 4, 5)

    val numbersRdd = sparkContext.parallelize(data)

    val sum = numbersRdd.reduce((n1, n2) => n1 + n2)
    sum shouldEqual 15
    // Logs contain some entries about jobs that are executed on executors (distributed manner)
    // It's enough to prove that reduce is a mixed task that intermediary results
    // are computed on executors and brought back to driver to prepare the final result
    val concatenatedLogs = logAppender.getMessagesText.mkString(",")
    concatenatedLogs.contains("Starting job: reduce at SparkExecutionTest.scala") shouldBe(true)
    concatenatedLogs.contains(",Final stage: ResultStage 0 (reduce at SparkExecutionTest.scala") shouldBe(true)
    concatenatedLogs.contains(",Job 0 finished: reduce at SparkExecutionTest.scala") shouldBe(true)
  }

  "collecting data" should "not make processing failing even if it uses not serializable object " in {
    val data = Array(1, 2, 3, 4, 5)

    val numbersRdd = sparkContext.parallelize(data)

    val collectedNumbers = numbersRdd
      .filter(number => number > 0)
      .collect()

    // Here data is first collected, i.e. it moves from
    // executors to driver and only on driver side the
    // multiplication is done. So there are no need to
    // send not serializable object
    val multiplicator = new NotSerializableMultiplicator(2)
    val result = multiplicator.multiply(collectedNumbers(0))

    result shouldBe 2
  }
}


class NotSerializableMultiplicator(value:Int) {

  def multiply(toMultiply:Int): Int = {
    value * toMultiply
  }

}