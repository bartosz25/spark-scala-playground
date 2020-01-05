package com.waitingforcode.structuredstreaming.kafka

import org.apache.spark.sql.SparkSession

/**
  * To test:
  * 1. Start a broker, use https://github.com/bartosz25/kafka-playground/tree/master/broker
  * 2. Login to the broker with ``
  * 3. Create 2 topics:
  *     ```
  *     kafka-topics.sh --create  --bootstrap-server localhost:9092 --topic spark_multiple_topics_1 --partitions 1 --replication-factor
  *     kafka-topics.sh --create  --bootstrap-server localhost:9092 --topic spark_multiple_topics_2 --partitions 1 --replication-factor
  *     ```
  * 4. Run this code.
  * 5. Read data from both topics:
  *    ```
  *    kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic spark_multiple_topics_1 --from-beginning
  *    kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic spark_multiple_topics_2 --from-beginning
  *    ```
  */
object KafkaMultipleTopicsOutput extends App {

  val sparkSession = SparkSession.builder()
    .appName("Spark-Kafka multiple topic output test").master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  val outputData = Seq(
    ("spark_multiple_topics_1", "a", "A"),
    ("spark_multiple_topics_1", "aa", "AA"),
    ("spark_multiple_topics_2", "b", "B")
  ).toDF("topic", "key", "value")

  outputData.write.option("kafka.bootstrap.servers", "210.0.0.20:9092").format("kafka").save()
}
