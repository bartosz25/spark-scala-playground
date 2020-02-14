package com.waitingforcode.structuredstreaming.multipletopics

import org.apache.spark.sql.execution.streaming.MemoryStream

object DataProducer {

  def produceDifferentSchemas(topic1: String, topic2: String, inputMemoryStream: MemoryStream[KafkaRecord]): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          inputMemoryStream.addData(Seq(
            KafkaRecord(topic1, System.currentTimeMillis().toString, """{"lower": "a", "upper": "A"}"""),
            KafkaRecord(topic2, System.currentTimeMillis().toString, """{"number": 1, "next_number": 2}"""),
            KafkaRecord(topic1, System.currentTimeMillis().toString, """{"lower": "b", "upper": "B"}"""),
            KafkaRecord(topic2, System.currentTimeMillis().toString, """{"number": 2, "next_number": 3}"""),
            KafkaRecord(topic1, System.currentTimeMillis().toString, """{"lower": "c", "upper": "C"}"""),
            KafkaRecord(topic2, System.currentTimeMillis().toString, """{"number": 3, "next_number": 4}""")
          ))
          Thread.sleep(2000L)
        }
      }
    }).start()
  }

  def produceSameSchemaDifferentTopics(topic1: String, topic2: String, inputMemoryStream: MemoryStream[KafkaRecord]): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          inputMemoryStream.addData(Seq(
            KafkaRecord(topic1, "new_order", """{"amount": 30.99, "user_id": 1}"""),
            KafkaRecord(topic2, "new_user", """{"user_id": 3"""),
            KafkaRecord(topic1, "new_order", """{"amount": 20.99, "user_id": 2}"""),
            KafkaRecord(topic1, "new_user", """{"user_id": 3"""),
            KafkaRecord(topic2, "new_order", """{"amount": 90.99, "user_id": 3}""")
          ))
          Thread.sleep(2000L)
        }
      }
    }).start()
  }
}
case class KafkaRecord(topic: String, key: String, value: String)
