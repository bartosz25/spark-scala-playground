package com.waitingforcode.structuredstreaming.fanout


import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConverters._

object FanOutPattern {

  val RunId = 4
  val JsonOutputDir = s"/tmp/fan-out/${RunId}/"

  def main(args: Array[String]): Unit = {
    println(s"Starting test at ${System.nanoTime()}")
    // Start producing data
    new Thread(DataProducer).start()

    try {
      SingleForwarder.execute()
    } catch {
      case e: Exception => e.printStackTrace()
    }

    // Check if the data was written
    val consumer = new KafkaConsumer[String, String](KafkaConfiguration.CommonConfiguration)
    import scala.collection.JavaConverters._
    consumer.subscribe(Seq(KafkaConfiguration.OutputTopic).asJava)

    def mapData(data: Iterator[ConsumerRecord[String, String]]): Seq[String] = {
      data.map(record => record.value()).toSeq.toList
    }
    val data = consumer.poll(Duration.of(1, ChronoUnit.MINUTES))
    val values = mapData(data.iterator().asScala)
    println(s"got ${values.size} records")
  }

}

object DataProducer extends Runnable {
  override def run(): Unit = {
    val kafkaAdmin = AdminClient.create(KafkaConfiguration.CommonConfiguration)
    println("Creating topics")
    try {
      val result = kafkaAdmin.createTopics(Seq(new NewTopic(KafkaConfiguration.OutgressTopic, 1, 1),
        new NewTopic(KafkaConfiguration.OutputTopic, 1, 1)).asJava)

      result.all().get(15, TimeUnit.SECONDS)
    } catch {
      case e: Exception => {
        // Check it but normally do not care about the exception. Probably it will be thrown at the second execution
        // because of the already existent topic
        e.printStackTrace()
      }
    }

    val producer = new KafkaProducer[String, String](KafkaConfiguration.CommonConfiguration)
    while (true) {
      (0 to 2)foreach(id => {
        producer.send(new ProducerRecord[String, String](KafkaConfiguration.OutgressTopic, s"key${id}", s"n=${System.nanoTime()}"))
      })
      producer.flush()
      Thread.sleep(2000L)
    }
  }
}


object KafkaConfiguration {
  val Broker = "kafka_broker:9092"
  val CommonConfiguration = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", Broker)
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("auto.offset.reset", "earliest")
    props.setProperty("group.id", "standard_client")
    props
  }

  val OutgressTopic = s"outgress_topic_1_${FanOutPattern.RunId}"
  val OutputTopic = s"outgress_output_${FanOutPattern.RunId}"
}
