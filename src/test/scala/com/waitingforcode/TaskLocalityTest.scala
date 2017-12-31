package com.waitingforcode

import com.waitingforcode.util.InMemoryLogAppender
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}


class TaskLocalityTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Spark task locality test").setMaster("local")
  var sparkContext:SparkContext = null

  before {
    sparkContext = SparkContext.getOrCreate(conf)
  }

  after {
    sparkContext.stop()
  }

  "node local levels" should "be resolved from driver host" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("Valid locality levels for TaskSet",
      "No tasks for locality level"))
    val preferredLocations = Seq("localhost", "executor_0.0.0.1.external_driver")
    new PreferenceAwareRDD(sparkContext, Seq.empty, preferredLocations)
      .map(url => (url, System.currentTimeMillis()))
      .foreach(pair => {
        println(s"Fetched ${pair._1} at ${pair._2}")
      })

    logAppender.getMessagesText() should contain allOf("Valid locality levels for TaskSet 0.0: PROCESS_LOCAL, NODE_LOCAL, ANY",
      "No tasks for locality level PROCESS_LOCAL, so moving to locality level NODE_LOCAL",
      "No tasks for locality level NODE_LOCAL, so moving to locality level ANY")
    println(s"caught messages ${logAppender.getMessagesText().mkString("\n")}")
  }

  "any levels" should "only resolved when no preferred host is defined" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("Valid locality levels for TaskSet",
      "No tasks for locality level"))
    new PreferenceAwareRDD(sparkContext, Seq.empty, Seq.empty)
      .map(url => (url, System.currentTimeMillis()))
      .foreach(pair => {
        println(s"Fetched ${pair._1} at ${pair._2}")
      })

    logAppender.getMessagesText() should contain allOf("Valid locality levels for TaskSet 0.0: NO_PREF, ANY",
      "No tasks for locality level NO_PREF, so moving to locality level ANY")
  }

}

class PreferenceAwareRDD(sparkContext: SparkContext, deps:Seq[Dependency[String]], preferredLocations: Seq[String])
  extends RDD[String](sparkContext, deps) {

  @DeveloperApi
  override def compute(partition: Partition, context: TaskContext): Iterator[String] = {
    Seq(s"test${System.currentTimeMillis()}").iterator
  }

  override protected def getPartitions: Array[Partition] = {
    val partition: Partition = DefaultPartition()
    Array(partition)
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = preferredLocations

  case class DefaultPartition() extends Partition {
    override def index: Int = 0
  }
}