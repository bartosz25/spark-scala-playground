package com.waitingforcode.structuredstreaming.corrupted_records

import org.apache.spark.sql.{SparkSession, functions}

object SentinelValuePattern extends App {

  // kafka-topics.sh --create  --bootstrap-server localhost:9092 --topic sentinel_value --partitions 2 --replication-factor 1
  val topic = "ignore_errors"
  val sparkSession = SparkSession.builder()
    .appName("Spark-Kafka corrupted records - sentinel value")
    .config("spark.sql.shuffle.partitions", "2")
    .master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  addCorruptedRecords(sparkSession, topic)

  val dataSource = sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "210.0.0.20:9092")
    .option("client.id", s"spark_sentinel_value")
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()

  val processedData = dataSource.selectExpr("CAST(value AS STRING) AS value_as_string")
    .withColumn("letter", functions.from_json($"value_as_string", schema, Map[String, String]()))
    .withColumn("letter", functions.when($"letter".isNotNull, $"letter")
      .otherwise(
        // Here I'm building our sentinel value
        functions.struct(
          functions.lit(".").as("lower"), functions.lit(".").as("upper")
        )
      )
    )
    .select("letter")

  val query = processedData.writeStream.format("console").option("truncate", "false")

  query.start().awaitTermination()

}
