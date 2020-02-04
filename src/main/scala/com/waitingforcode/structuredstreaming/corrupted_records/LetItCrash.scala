package com.waitingforcode.structuredstreaming.corrupted_records

import org.apache.spark.sql.{SparkSession, functions}

object LetItCrash extends App {

  // kafka-topics.sh --create  --bootstrap-server localhost:9092 --topic let_it_crash --partitions 2 --replication-factor 1
  val topic = "let_it_crash"
  val sparkSession = SparkSession.builder()
    .appName("Spark-Kafka corrupted records - let it crash")
    .config("spark.sql.shuffle.partitions", "2")
    .master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._


  addCorruptedRecords(sparkSession, topic)

  val dataSource = sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "210.0.0.20:9092")
    .option("client.id", s"spark_let_it_crash")
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()

  val jsonOptions = Map[String, String](
      // Doesn't work like expected!
      // Simply speaking, this parameter is used only for the data sources readers
      // and cannot be injected here
      //"columnNameOfCorruptRecord" -> "corrupted_records",
      // Also doesn't work, for the same reason
      //"dropFieldIfAllNull" -> "true"
  )
  val processedData = dataSource.selectExpr("CAST(value AS STRING) AS value_as_string")
    .withColumn("letter", functions.from_json($"value_as_string", schema, jsonOptions))

  val query = processedData
    .writeStream
    .foreachBatch((dataset, batchNumber) => {
      dataset.persist()

      // I'm using take because it fails for .first() when used with empty dataset with:
      // "Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: next on empty iterator"
      // error
      val failedRow = dataset.filter("letter IS NULL").select("value_as_string").take(1)
      if (failedRow.nonEmpty) {
        throw new RuntimeException(
          s"""An error happened when converting "${failedRow(0).getAs[String]("value_as_string")}" to JSON."""
        )
      }
      println("Writing rows to the console")
      // Otherwise, do something with data
      dataset.show(false)

      dataset.unpersist()
    })

  query.start().awaitTermination()

}
