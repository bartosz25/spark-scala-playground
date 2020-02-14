package com.waitingforcode.structuredstreaming.multipletopics

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.execution.streaming.MemoryStream

object DifferentInputTopicFilter extends App {

  val topic1 = "topic1"
  val topic2 = "topic2"
  val sparkSession = SparkSession.builder()
    .appName("Spark-Kafka different schemas").master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  val inputMemoryStream = new MemoryStream[KafkaRecord](1, sparkSession.sqlContext)
  DataProducer.produceDifferentSchemas(topic1, topic2, inputMemoryStream)

  import org.apache.spark.sql.types._
  val schemaLetters = new StructType()
    .add($"lower".string.copy(nullable = false))
    .add($"upper".string.copy(nullable = false))
  val schemaNumbers = new StructType()
    .add($"number".int.copy(nullable = false))
    .add($"next_number".int.copy(nullable = false))

  inputMemoryStream.toDS()
    .select($"topic", $"value".cast("string"))
    .writeStream
    .foreachBatch((dataset, _) => {
      dataset.persist()
      dataset.filter($"topic" === topic1)
        .select(functions.from_json($"value".cast("string"), schemaLetters).as("value")).select("value.*")
        .selectExpr("CONCAT('letters=', lower, '/',  upper)")
        .show(false)
      dataset.filter($"topic" === topic2)
        .select(functions.from_json($"value".cast("string"), schemaNumbers).as("value")).select("value.*")
        .selectExpr("CONCAT('numbers=', number, '/',  next_number)")
        .show(false)
      dataset.unpersist()
    })
   .start().awaitTermination()
}
