package com.waitingforcode.structuredstreaming.multipletopics

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{SparkSession, functions}

object DifferentInputEventFilter extends App {

  val topic1 = "topic1"
  val topic2 = "topic2"
  val sparkSession = SparkSession.builder()
    .appName("Spark-Kafka different schemas").master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  val inputMemoryStream = new MemoryStream[KafkaRecord](1, sparkSession.sqlContext)
  DataProducer.produceSameSchemaDifferentTopics(topic1, topic2, inputMemoryStream)

  import org.apache.spark.sql.types._
  val schema = new StructType()
    .add($"amount".double.copy(nullable = false))
    .add($"user_id".int.copy(nullable = false))

  inputMemoryStream.toDS()
    .filter("key = 'new_order'")
    .select($"topic",
      functions.from_json($"value".cast("string"), schema, Map[String, String]()).as("order")
    )
    .filter("order.amount > 25")
    .writeStream.format("console").option("truncate", "false")
    .start().awaitTermination()
}
