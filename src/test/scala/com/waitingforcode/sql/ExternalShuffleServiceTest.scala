package com.waitingforcode.sql

import com.waitingforcode.util.InMemoryLogAppender
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}


class ExternalShuffleServiceTest extends FlatSpec with Matchers {

  val logAppender = InMemoryLogAppender.createLogAppender(Seq("shuffle service",
    "Registered streamId",
    "Received message ChunkFetchRequest", "Received request: OpenBlocks",
    "Handling request to send map output"))
  private val sparkSession: SparkSession = SparkSession.builder().appName("SaveMode test").master("spark://localhost:7077")
    .config("spark.shuffle.service.enabled", true)
    .getOrCreate()

  "shuffle files" should "be read with external shuffle fetcher" in {
    import sparkSession.implicits._
    val numbers = (1 to 100).toDF("nr").repartition(10).repartition(15)

    // Only to trigger the computation
    numbers.collect()

    val classWithMessage = logAppender.messages
        .groupBy(logMessage => logMessage.loggingClass)
        .map(groupClassNameWithMessages => (groupClassNameWithMessages._1,
          groupClassNameWithMessages._2.map(logMessage => logMessage.message)))
    val blockManagerMessage = classWithMessage("org.apache.spark.storage.BlockManager")
    // Block manager controls if the files can be removed after stopping the executor
    // It also defines from where shuffle files will be fetched. Throughout below message we
    // can see that the fetch will occur from external shuffle service
    blockManagerMessage should have size 1
    blockManagerMessage(0) shouldEqual "external shuffle service port = 7337"
    // In such case we can only see the activity of below classes and exchanges. The executors
    // aren't registered with external shuffle server in standalone mode because of this check from
    // BlockManager class: if (externalShuffleServiceEnabled && !blockManagerId.isDriver) {
    //  registerWithExternalShuffleServer()
    // }
    // So we check the existence of fetch messages
    val nettyBlockRpcServerMessages = classWithMessage("org.apache.spark.network.netty.NettyBlockRpcServer")
    val openBlocksMessagePresent = nettyBlockRpcServerMessages.find(_.contains("Received request: OpenBlocks"))
    openBlocksMessagePresent.nonEmpty shouldBe true
    val messageDecoderMessages = classWithMessage("org.apache.spark.network.protocol.MessageDecoder")
    val fetchChunkRequestPresent = messageDecoderMessages.find(_.contains("ChunkFetchRequest: ChunkFetchRequest{streamChunkId"))
    fetchChunkRequestPresent.nonEmpty shouldBe true
  }

}
