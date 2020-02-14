package com.waitingforcode.structuredstreaming.multipletopics

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{SparkSession, functions}

object DifferentInputBigSchema extends App {

  val topic1 = "topic1"
  val topic2 = "topic2"
  val sparkSession = SparkSession.builder()
    .appName("Spark-Kafka different schemas").master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  val inputMemoryStream = new MemoryStream[KafkaRecord](1, sparkSession.sqlContext)
  DataProducer.produceDifferentSchemas(topic1, topic2, inputMemoryStream)

  case class SharedEntryData(lower: Option[String], upper: Option[String],
                             number: Option[Int], next_number: Option[Int]) {
    def topic1Label = s"${lower.orNull} / ${upper.orNull}"
    def topic2Label = s"${number.orNull} / ${next_number.orNull}"
  }
  case class SharedEntry(topic: String, data: SharedEntryData) {
    def toCommonFormat: CommonFormat = {
      if (topic == topic1) {
        CommonFormat(s"letters=${data.topic1Label}")
      } else {
        CommonFormat(s"numbers=${data.topic2Label}")
      }
    }
  }
  case class CommonFormat(label: String)
  import org.apache.spark.sql.types._

  inputMemoryStream.toDS()
    .withColumn("shared_entry", functions.from_json($"value".cast("string"),
      ScalaReflection.schemaFor[SharedEntryData].dataType.asInstanceOf[StructType], Map[String, String]()))
    .select($"topic",  $"shared_entry".as("data")).as[SharedEntry]
    .map(sharedSchema => {
      sharedSchema.toCommonFormat
    })
    .writeStream.format("console").option("truncate", "false")
    .start().awaitTermination()

}
