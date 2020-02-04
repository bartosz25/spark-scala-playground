package com.waitingforcode.structuredstreaming.corrupted_records

import org.apache.spark.sql.{Row, SparkSession, functions}

object Wrapper extends App {

  // kafka-topics.sh --create  --bootstrap-server localhost:9092 --topic wrapper --partitions 2 --replication-factor 1
  val topic = "wrapper"
  val sparkSession = SparkSession.builder()
    .appName("Spark-Kafka corrupted records - wrapper")
    .config("spark.sql.shuffle.partitions", "2")
    .master("local[*]")
    .getOrCreate()
  import sparkSession.implicits._

  addCorruptedRecords(sparkSession, topic)

  val dataSource = sparkSession.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "210.0.0.20:9092")
    .option("client.id", s"wrapper")
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()

  val processedData = dataSource.selectExpr("CAST(value AS STRING) AS value_as_string")
    .withColumn("letter", functions.from_json($"value_as_string", schema, Map[String, String]()))
    .map(row => {
      val letter = row.getAs[Row]("letter")
      if (letter != null) {
        val sentinelData = Option(letter).map(letterData => SentinelForDLQData(
          lower = letterData.getAs[String]("lower"), upper = letterData.getAs[String]("upper"))
        )
        SentinelForDLQWrapper(data = sentinelData)
      } else {
        SentinelForDLQWrapper(data = None, error = Some(
          SentinelForDLQError(row.getAs[String]("value_as_string"))
        ))
      }
    })

  val query = processedData.writeStream.format("console").option("truncate", "false")

  query.start().awaitTermination()
}

case class SentinelForDLQWrapper(data: Option[SentinelForDLQData] = None,
                                 error: Option[SentinelForDLQError] = None)
case class SentinelForDLQData(lower: String, upper: String)
case class SentinelForDLQError(raw: String)
