package com.waitingforcode.memoryimpact

import com.waitingforcode.util.InMemoryLogAppender
import org.apache.spark.storage.StorageLevel
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class MemoryImpactCacheTest extends FlatSpec with BeforeAndAfterAll with Matchers {

  override def beforeAll() {
    prepareTestEnvironment()
  }

  "memory-only cache" should "not fail when there is too few memory in sequential processing" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("Will not store", "Not enough space to cache"))

    val textRdd = parallelProcessingSession.sparkContext.textFile(Test1GbFile)

    textRdd.persist(StorageLevel.MEMORY_ONLY)
    processTextRdd(textRdd)

    // In the logs we can see what happens if we ask Spark to cache the data when there is not enough memory
    // The message we can find are:
    // Will not store rdd_1_8
    // Not enough space to cache rdd_1_8 in memory! (computed 190.7 MB so far)
    // Will not store rdd_1_11
    // Not enough space to cache rdd_1_11 in memory! (computed 190.7 MB so far)
    logAppender.getMessagesText().find(message => message.startsWith("Will not store rdd_1_")) shouldBe defined
    logAppender.getMessagesText().find(message => message.startsWith("Not enough space to cache rdd_") &&
      message.contains(" in memory! (computed ")) shouldBe defined
  }

  "memory-disk cache" should "store some data in memory and the rest on disk" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("stored as values in memory", "Not enough space to cache",
      "to disk instead", "on disk in", "on disk on"))

    val textRdd = sequentialProcessingSession.sparkContext.textFile(Test1GbFile)

    textRdd.persist(StorageLevel.MEMORY_AND_DISK)
    processTextRdd(textRdd)
    val rddCacheStoredInMemory = logAppender.getMessagesText().find(message => message.startsWith("Block rdd_1_0 stored as values in memory"))
    rddCacheStoredInMemory shouldBe defined
    val warnNotEnoughMemoryToCache = logAppender.getMessagesText().find(message => message.startsWith("Not enough space to cache rdd_"))
    warnNotEnoughMemoryToCache shouldBe defined
    val attemptToDiskCache = logAppender.getMessagesText().find(message => message.contains("Persisting block")
      && message.endsWith("to disk instead."))
    attemptToDiskCache shouldBe defined
    val successfulDiskCache = logAppender.getMessagesText().find(message => message.startsWith("Block rdd_1") && message.contains("file on disk in"))
    successfulDiskCache shouldBe defined
  }

  "failed memory cache" should "recompute RDD if used again" in {
    // Same code as the first test but with double call to processTextRdd to see what happens in the logs
    // when we're unable to cache and despite that we use the same RDD twice
    // Locally it was rdd_1_26 block that weren't cached. It may be different but for test's simplicity I keep
    // hardcoded id
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("rdd_1_26", "872415232+33554432"))
    val sequentialSession = sequentialProcessingSession

    val textRdd = sequentialSession.sparkContext.textFile(Test1GbFile)

    textRdd.persist(StorageLevel.MEMORY_ONLY)

    processTextRdd(textRdd)
    processTextRdd(textRdd)

    // As you can observe through the log events, some RDD blocks aren't cached because of insufficient free space
    // in memory. In such case Spark simply ignores it, as we saw earlier in the tests, and instead recomputes the
    // data
    // You can notice the recomputation by the presence of the log starting with "Input split". For cached blocks
    // as for instance the first one, we'll find only 1 entry for
    // "Input split: file:/tmp/spark-memory-impact/generated_file_1_gb.txt:872415232+33554432"
    val groupedLogMessages = logAppender.getMessagesText().groupBy(message => message)
    groupedLogMessages("Task 59 trying to put rdd_1_26") should have size 1
    groupedLogMessages("Task 26 trying to put rdd_1_26") should have size 1
    groupedLogMessages("Block rdd_1_26 could not be removed as it was not found on disk or in memory") should have size 2
    groupedLogMessages("Input split: file:/tmp/spark-memory-impact/generated_file_1_gb.txt:872415232+33554432") should have size 2
  }

}
