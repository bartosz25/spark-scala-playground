package com.waitingforcode.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.{FlatSpec, Matchers}

class ProcessingAbstractionTest extends FlatSpec with Matchers {

  private val topicName = "raw_data"
  private val kafkaBroker = "160.0.0.20:9092"

  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("Processing abstraction test").master("local[*]").getOrCreate()
  import sparkSession.implicits._

  behavior of "Apache Kafka source"

  it should "be processed with Dataset batch abstraction" in {
    val allRows = sparkSession.read.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("client.id", s"sessioni zation-demo-streaming")
      .option("subscribe", topicName)
      .option("checkpointLocation", s"/tmp/test/checkpoint")
      .load()
      .map(row => row.getAs[Long]("offset"))
      .count()

    println(s"allRows=${allRows}")
    allRows should be > 0L
  }

  it should "be processed with Dataset streaming abstraction" in {
    val query = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("client.id", s"sessionization-demo-streaming")
      .option("subscribe", topicName)
      .load()
      .map(row => row.getAs[String]("topic"))
      .groupBy("value").count()


    query.writeStream.outputMode(OutputMode.Complete()).option("checkpointLocation", "/tmp/test-kafka")
      .format("console")
      .start().awaitTermination(45000L)
  }

}
