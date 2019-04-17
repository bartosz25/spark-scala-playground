package com.waitingforcode.structuredstreaming.fanin

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * The code illustrates the at-least once delivery semantic with Apache Spark Structured Streaming and
  * Apache Kafka implementation of fan-in ingress pattern.
  *
  * To run the code:
  * 1. Start the Apache Kafka broker (see TODO: add link to the post here)
  * 2. Run the code for the first time.
  * 3. Check the duplicatedEvents print. You shouldn't see some duplicated entries here.
  * 4. Rerun the code and set the [[FanInIngressPattern.FailureFlag]] to false
  * 5. Check the duplicatedEvents print. You should see some duplicated entries here.
  */
object FanInIngressPattern {

  val FailureFlag = true
  val RunId = 1
  val JsonMapper = new ObjectMapper()
  JsonMapper.registerModule(DefaultScalaModule)

  def main(args: Array[String]): Unit = {
    println(s"Starting test at ${System.nanoTime()}")
    // Start producing data
    new Thread(DataProducer).start()

    try {
      val sparkSession = SparkSession.builder()
        .appName("Spark-Kafka fan-in ingress pattern test").master("local[*]")
        .getOrCreate()
      val dataFrame = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KafkaConfiguration.Broker)
        .option("checkpointLocation", s"/tmp/checkpoint_consumer_${RunId}")
        .option("client.id", s"client#${System.currentTimeMillis()}")
        .option("subscribe", s"${KafkaConfiguration.IngressTopic1}, ${KafkaConfiguration.IngressTopic2}")
        .load()
      import sparkSession.implicits._
      val query = dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .groupByKey(row => row.getAs[String]("key"))
        .mapGroups {
          case (key, rows) => {
            val ascOrderEvents = if (FailureFlag) {
              rows.map(row => {
                  val inputMessage = JsonMapper.readValue(row.getAs[String]("value"), classOf[InputMessage])
                  inputMessage.value
              })
            } else {
              rows.map(row => {
                Try {
                  val inputMessage = JsonMapper.readValue(row.getAs[String]("value"), classOf[InputMessage])
                  inputMessage.value
                }
              })
                .filter(triedConversion => triedConversion.isSuccess)
                .map(jsonValue => jsonValue.get)
            }
            ConsolidatedEvents(key, ascOrderEvents.toSeq.sorted)
          }
        }.selectExpr("key", "CAST(ascOrderedEvents AS STRING) AS value")
        .writeStream
        .option("checkpointLocation", s"/tmp/checkpoint_producer_${RunId}")
        .format("kafka")
        .option("kafka.bootstrap.servers", KafkaConfiguration.Broker)
        .option("topic", KafkaConfiguration.ConsolidatedTopic)
        .start()
      query.awaitTermination(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES))
      query.stop()
    } catch {
      case e: Exception => e.printStackTrace()
    }

    // Check what data was written
    val consumer = new KafkaConsumer[String, String](KafkaConfiguration.CommonConfiguration)
    import scala.collection.JavaConverters._
    consumer.subscribe(Seq(KafkaConfiguration.ConsolidatedTopic).asJava)

    def mapData(data: Iterator[ConsumerRecord[String, String]]): Seq[String] = {
      data.map(record => record.value()).toSeq.toList
    }
    val data = consumer.poll(Duration.of(1, ChronoUnit.MINUTES))
    val values = mapData(data.iterator().asScala)
    val duplicatedEvents = values.groupBy(value => value)
      .filter {
        case (_, values) => values.size > 1
      }
    println(s"found duplicated events=${duplicatedEvents.mkString("\n")}")
  }

}

case class ConsolidatedEvents(key: String, ascOrderedEvents: Seq[String])

case class InputMessage(value: String)

object DataProducer extends Runnable {
  override def run(): Unit = {
    val kafkaAdmin = AdminClient.create(KafkaConfiguration.CommonConfiguration)
    println("Creating topics")
    try {
      val result = kafkaAdmin.createTopics(Seq(new NewTopic(KafkaConfiguration.IngressTopic1, 1, 1),
        new NewTopic(KafkaConfiguration.IngressTopic2, 1, 1), new NewTopic(KafkaConfiguration.ConsolidatedTopic, 1, 1)).asJava)

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
        val message = FanInIngressPattern.JsonMapper.writeValueAsString(InputMessage(s"n=${System.nanoTime()}"))
        producer.send(new ProducerRecord[String, String](KafkaConfiguration.IngressTopic1, message))
      })
      (1 to 3)foreach(id => {
        val message = if (FailureController.canFail()) {
          "}"
        } else {
          FanInIngressPattern.JsonMapper.writeValueAsString(InputMessage(s"n=${System.nanoTime()}"))
        }
        producer.send(new ProducerRecord[String, String](KafkaConfiguration.IngressTopic2, s"key${id}", message))
      })
      producer.flush()
      Thread.sleep(2000L)
    }
  }
}


object KafkaConfiguration {
  val Broker = "171.18.0.20:9092"
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

  val IngressTopic1 = s"ingress_topic_1_${FanInIngressPattern.RunId}"
  val IngressTopic2 = s"ingress_topic_2_${FanInIngressPattern.RunId}"
  val ConsolidatedTopic = s"consolidated_topic_${FanInIngressPattern.RunId}"
}

object FailureController {

  private val CheckNumbers = new AtomicInteger(0)

  def canFail(): Boolean = {
    if (CheckNumbers.get() == 10 && FanInIngressPattern.FailureFlag) {
      true
    } else {
      CheckNumbers.incrementAndGet()
      false
    }
  }
}
