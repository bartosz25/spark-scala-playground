package com.waitingforcode.structuredstreaming.fanout

import java.util.concurrent.TimeUnit

import com.waitingforcode.structuredstreaming.fanout.FanOutPattern.RunId
import org.apache.spark.sql.SparkSession

object MultipleForwarders {

  object JsonForwarder {
    def main(args: Array[String]): Unit = {
      val sparkSession = SparkSession.builder()
        .appName("Spark-Kafka fan-out ingress pattern test - JSON forwarder").master("local[*]")
        .getOrCreate()
      val dataFrame = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KafkaConfiguration.Broker)
        .option("checkpointLocation", s"/tmp/checkpoint_consumer_${RunId}")
        .option("client.id", s"client#${System.currentTimeMillis()}")
        .option("subscribe", KafkaConfiguration.OutgressTopic)
        .load()

      val query = dataFrame.writeStream.foreachBatch((dataset, batchId) => {
        dataset.write.json(s"/tmp/fan-out/${RunId}/output${batchId}")
      }).start()
      query.awaitTermination(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES))
      query.stop()
    }
  }

  object KafkaForwarder {
    def main(args: Array[String]): Unit = {
      val sparkSession = SparkSession.builder()
        .appName("Spark-Kafka fan-out ingress pattern test - Kafka forwarder").master("local[*]")
        .getOrCreate()
      val dataFrame = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KafkaConfiguration.Broker)
        .option("checkpointLocation", s"/tmp/checkpoint_consumer_${RunId}")
        .option("client.id", s"client#${System.currentTimeMillis()}")
        .option("subscribe", KafkaConfiguration.OutgressTopic)
        .load()
      // import sparkSession.implicits._
      val query = dataFrame.writeStream
        .option("checkpointLocation", s"/tmp/checkpoint_producer_fanout_${RunId}")
        .format("kafka")
        .option("kafka.bootstrap.servers", KafkaConfiguration.Broker)
        .option("topic", KafkaConfiguration.OutputTopic)
        .start()
      query.awaitTermination(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES))
      query.stop()
    }
  }

}
