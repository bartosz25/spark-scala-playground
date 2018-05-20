package com.waitingforcode.structuredstreaming

import com.waitingforcode.util.{InMemoryLogAppender, LogMessage}
import org.apache.log4j.spi.LoggingEvent
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{FlatSpec, Matchers}

class ContinuousProcessingTest extends FlatSpec with Matchers {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming output modes - mapGroupsWithState")
    .master("local[*]").getOrCreate()
  import sparkSession.implicits._

  "continuous execution" should "execute trigger at regular interval independently on data processing" in {
    val messageSeparator = "SEPARATOR"
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("New epoch", "has offsets reported from all partitions",
      "has received commits from all partitions. Committing globally.", "Committing batch"),
      (loggingEvent: LoggingEvent) => LogMessage(s"${loggingEvent.timeStamp} ${messageSeparator} ${loggingEvent.getMessage}", ""))
    val rateStream = sparkSession.readStream.format("rate").option("rowsPerSecond", "10")
      .option("numPartitions", "2")
    val numbers = rateStream.load()
      .map(rateRow => {
        // Slow down the processing to show that the epochs don't wait commits
        Thread.sleep(500L)
        ""
      })

    numbers.writeStream.outputMode("append").trigger(Trigger.Continuous("2 second")).format("memory")
      .queryName("test_accumulation").start().awaitTermination(60000L)

    // A sample output of accumulated messages could be:
    // 13:50:58 CET 2018 :  New epoch 1 is starting.
    // 13:51:00 CET 2018 :  New epoch 2 is starting.
    // 13:51:00 CET 2018 :  Epoch 0 has offsets reported from all partitions: ArrayBuffer(
    //  RateStreamPartitionOffset(1,-1,1521958543518), RateStreamPartitionOffset(0,-2,1521958543518))
    // 13:51:00 CET 2018 :  Epoch 0 has received commits from all partitions. Committing globally.
    // 13:51:01 CET 2018 :  Committing batch 0 to MemorySink
    // 13:51:01 CET 2018 :  Epoch 1 has offsets reported from all partitions: ArrayBuffer(
    //  RateStreamPartitionOffset(0,24,1521958546118), RateStreamPartitionOffset(1,25,1521958546118))
    // 13:51:01 CET 2018 :  Epoch 1 has received commits from all partitions. Committing globally.
    // 13:51:01 CET 2018 :  Committing batch 1 to MemorySink
    // 13:51:02 CET 2018 :  New epoch 3 is starting.
    // 13:51:04 CET 2018 :  New epoch 4 is starting.
    // 13:51:06 CET 2018 :  New epoch 5 is starting.
    // 13:51:08 CET 2018 :  New epoch 6 is starting.
    // As you can see, the epochs start according to the defined trigger and not after committing given epoch.
    // Even if given executor is in late for the processing, it polls the information about the epochs regularly through
    // org.apache.spark.sql.execution.streaming.continuous.EpochPollRunnable
    var startedEpoch = -1
    var startedEpochTimestamp = -1L
    for (message <- logAppender.getMessagesText()) {
      val Array(msgTimestamp, messageContent, _ @_*) = message.split(messageSeparator)
      if (messageContent.contains("New epoch")) {
        // Here we check that the epochs are increasing
        val newEpoch = messageContent.split(" ")(3).toInt
        val startedNewEpochTimestamp = msgTimestamp.trim.toLong
        newEpoch should be > startedEpoch
        startedNewEpochTimestamp should be > startedEpochTimestamp
        startedEpoch = newEpoch
        startedEpochTimestamp = startedNewEpochTimestamp
      } else if (messageContent.contains("Committing globally")) {
        // Here we prove that the epoch commit is decoupled from the starting of the new
        // epoch processing. In fact the new epoch starts according to the delay defined
        // in the continuous trigger but the commit happens only when all records are processed
        // And both concepts are independent
        val committedEpoch = messageContent.split(" ")(2).toInt
        startedEpoch should be > committedEpoch
        msgTimestamp.trim.toLong should be > startedEpochTimestamp
      }
    }
  }
}
