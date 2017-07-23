package com.waitingforcode

import com.waitingforcode.util.InMemoryLogAppender
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class BlockTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Spark block test").setMaster("local[*]")
  var sparkContext:SparkContext = null

  before {
    sparkContext = SparkContext.getOrCreate(conf)
  }

  after {
    sparkContext.stop
  }

  "simple processing with grouping" should "accumulate basic logs" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("Getting local block", "Level for block",
      "Getting 5 non-empty blocks", "Updated info of block", "in memory on", "Block", "Started 0 remote fetches",
      "Told master about block broadcast_1_piece0"))

    val data = sparkContext.parallelize(1 to 100, 5)

    data.map(number => (number%2, number))
      .groupByKey(3)
      .foreach(number => {
        println(s"Number=${number}")
      })

    val logMessages = logAppender.getMessagesText()
    // This log appears when worker sends the UpdateBlockInfo message to
    // the master's block manager. The UpdateBlockInfo message contains the
    // information about block's id, storage level, the size taken in memory and on disk
    logMessages should contain ("Updated info of block broadcast_0_piece0")
    // This message tells that the block manager tries to retrieve the block
    // locally. If the block is not found locally, it's later fetched
    // from remote block manager
    logMessages should contain ("Getting local block broadcast_0")
    // Here the block manager informs the master about the state
    // of given block. It's important since sometimes the expected storage
    // level can not be met (e.g. MEMORY+DISK is demanded but only DISK is
    // written) and thanks to the message, the master will know that
    logMessages should contain ("Told master about block broadcast_1_piece0")
    // The 2 logs below represent the shuffle operation. The first one is printed
    // when the ShuffleBlockFetcherIterator iterates among all blocks to fetch and
    // resolves which ones are stored locally and which ones remotely.
    // The 2nd log is printed when fetchnig of shuffle remote blocks begins.
    logMessages should contain ("Getting 5 non-empty blocks out of 5 blocks")
    logMessages.find(log => log.startsWith("Started 0 remote fetches")) should not be empty;
  }

  "the processing with replicated cache" should "generate logs showing the replication" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("Level for block rdd_0_1",
      "Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication",
      "Replicating rdd_0_0 of",
      "Block rdd_0_1 replicated to only 0",
      "Block rdd_0_0 replicated to only 0"))


    val data = sparkContext.parallelize(1 to 100, 5)

    data.persist(StorageLevel.MEMORY_ONLY_2).map(number => (number%2, number))
      .groupByKey(3)
      .foreach(number => {
        println(s"Number=${number}")
      })

    val logMessages = logAppender.getMessagesText()
    // The information about expected replication level is shown in the logs
    // when block's data is retrieved from the local block manager
    logMessages should contain ("Level for block rdd_0_1 is StorageLevel(memory, deserialized, 2 replicas)")
    // Here block manager indicates that it's doing the replication of given block (rdd_0_0 in example)
    // to some number of nodes in the cluster. Below log contains 0 as its number since the
    // code is executed against local master. The value should be 1 in the real cluster of 2 nodes at least.
    logMessages.find(log => log.startsWith("Replicating rdd_0_0 of 40 bytes to 0 peer(s)")) should not be empty;
    // This log is a warning message when the number of replicated blocks doesn't met the
    // expectation of storage level
    logMessages should contain ("Block rdd_0_0 replicated to only 0 peer(s) instead of 1 peers")
  }

}
