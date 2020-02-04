package com.waitingforcode.structuredstreaming.corrupted_records

import org.apache.spark.sql.{Row, SparkSession, functions}

object IgnoreErrorsLogging extends App {

  // kafka-topics.sh --create  --bootstrap-server localhost:9092 --topic ignore_errors_logging --partitions 2 --replication-factor 1
  val topic = "ignore_errors_logging"
  val sparkSession = SparkSession.builder()
    .appName("Spark-Kafka corrupted records - ignore errors with logging")
    .config("spark.sql.shuffle.partitions", "2")
    .master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  addCorruptedRecords(sparkSession, topic)

  val dataSource = sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "210.0.0.20:9092")
    .option("client.id", s"spark_ignore_errors_logging")
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()

  val processedData = dataSource.selectExpr("CAST(value AS STRING) AS value_as_string")
    .withColumn("letter", functions.from_json($"value_as_string", schema))
    .filter(row => {
      val convertedLetter = row.getAs[Row]("letter")
      if (convertedLetter == null) {
        println(s"Record ${row.getAs[String]("value_as_string")} cannot be converted")
        false
      } else {
        true
      }
    })

  val query = processedData.writeStream.format("console").option("truncate", "false")

  query.start().awaitTermination()
}
