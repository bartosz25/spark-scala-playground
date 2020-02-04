package com.waitingforcode.structuredstreaming

import org.apache.spark.sql.SparkSession

package object corrupted_records {

  import org.apache.spark.sql.types._
  val schema = new StructType()
    .add(StructField("lower", StringType, false))
    .add(StructField("upper", StringType, false))

  def addCorruptedRecords(sparkSession: SparkSession, topic: String) = {
    import sparkSession.implicits._
    val inputKafkaData = sparkSession.createDataset(Seq(
      (topic, System.currentTimeMillis().toString, """{"lower": "a", "upper": "A"}"""),
      (topic, System.currentTimeMillis().toString, """{"PARTIAL_lower": "a", "upper": "A"}"""),
      (topic, System.currentTimeMillis().toString, """not a JSON"""),
      (topic, System.currentTimeMillis().toString, """{"SOME_LETTER": "a", OTHER_LETTER": "A"}""")
    )).toDF("topic", "key", "value")

    // I'm using here the broker from my other project:
    // https://github.com/bartosz25/kafka-playground/tree/master/broker
    inputKafkaData.write.format("kafka").option("kafka.bootstrap.servers", "210.0.0.20:9092").save()
  }

}
