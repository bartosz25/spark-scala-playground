package com.waitingforcode.structuredstreaming.fanout

import java.util.concurrent.TimeUnit

import com.waitingforcode.structuredstreaming.fanout.FanOutPattern.RunId
import org.apache.spark.sql.SparkSession

object SingleForwarder {

  def execute(): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark-Kafka fan-out ingress pattern test - single forwarder").master("local[*]")
      .getOrCreate()
    val dataFrame = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaConfiguration.Broker)
      .option("checkpointLocation", s"/tmp/checkpoint_consumer_${RunId}")
      .option("client.id", s"client#${System.currentTimeMillis()}")
      .option("subscribe", KafkaConfiguration.OutgressTopic)
      .load()
    // import sparkSession.implicits._
    val query = dataFrame
      .writeStream
      .foreachBatch((dataset, batchId) => {
        dataset.write.json(s"${FanOutPattern.JsonOutputDir}output${batchId}")
        dataset.write
          .option("checkpointLocation", s"/tmp/checkpoint_producer_fanout_${RunId}")
          .format("kafka")
          .option("kafka.bootstrap.servers", KafkaConfiguration.Broker)
          .option("topic", KafkaConfiguration.OutputTopic).save()
      })
      .start()
    query.awaitTermination(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES))
    query.stop()
  }

}

