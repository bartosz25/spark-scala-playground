package com.waitingforcode.structuredstreaming.corrupted_records

import org.apache.spark.sql.{SparkSession, functions}

object DeadLetterQueue extends App {

  // kafka-topics.sh --create  --bootstrap-server localhost:9092 --topic dead_letter_queue --partitions 2 --replication-factor 1
  val topic = "ignore_errors_logging"
  val sparkSession = SparkSession.builder()
    .appName("Spark-Kafka corrupted records - Dead Letter Queue")
    .config("spark.sql.shuffle.partitions", "2")
    .master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  addCorruptedRecords(sparkSession, topic)

  val dataSource = sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "210.0.0.20:9092")
    .option("client.id", s"spark_dead_letter_queue")
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()

  val processedData = dataSource.selectExpr("CAST(value AS STRING) AS value_as_string")
    .withColumn("letter", functions.from_json($"value_as_string", schema))

  val query = processedData
    .writeStream
    .foreachBatch((dataset, batchNumber) => {
      dataset.persist()

      // I'm simply printing both, valid and invalid, datasets
      // But it's only for demo purposes. You will probably want to write
      // them somewhere else
      println("Corrupted records")
      dataset.filter("letter IS NULL").show(false)
      println("Valid records")
      dataset.filter("letter IS NOT NULL").show(false)

      dataset.unpersist()
    })

  query.start().awaitTermination()
}
