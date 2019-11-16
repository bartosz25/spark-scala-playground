package com.waitingforcode.sparksummit2019.customstatestore

import java.util.concurrent.ConcurrentLinkedQueue

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded
import com.waitingforcode.sparksummit2019.customstatestore.DynamoDbStateStoreParams._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreCustomMetric, StateStoreCustomSumMetric, StateStoreId, StateStoreMetrics, StateStoreProvider, UnsafeRowPair}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SizeEstimator

import scala.collection.mutable

/**
  * *** ONLY FOR A DEMONSTRATION PURPOSE ***
  *
  * A simple implementation class for a custom state store. I'm using here an embedded DynamoDB instance
  * but since it uses the same API like the real AWS database, it should be easily tested in the real environment.
  *
  * The goal of this code is twofold:
  * - explore the API and demonstrate how to extend a state store
  * - try a less memory-intensive approach where not all the state is stored on the main memory
  *
  */

class LocalDynamoDbStateStoreProvider extends StateStoreProvider with Logging {

  // I use the same values as in HDFSBackedStateStoreProvider
  // Except for: hadoopConf and numberOfVersionsToRetainInMemory because the internal logic won't use them
  @volatile private var stateStoreId_ : StateStoreId = _
  @volatile private var keySchema: StructType = _
  @volatile private var valueSchema: StructType = _
  @volatile private var storeConf: StateStoreConf = _

  // It would be better to have these fields in an object. The problem is that the state store is partition-based
  // and the object is located on a single JVM, so it'll be shared across partitions.
  private lazy val UpdatesList = new ConcurrentLinkedQueue[StateStoreChange]()

  private lazy val StatesWithVersions = new mutable.HashMap[String, SnapshotGroupForKey]()

  // Why I'm declaring custom variables? Check the supportedMetrics method
  private lazy val customMetricFlushes = StateStoreCustomSumMetric("flushCalls",
    "how many times the flush requests were invoked")
  private lazy val customMetricGets = StateStoreCustomSumMetric("getCalls",
    "how many times DynamoDB getItem query was issued")


  override def init(stateStoreId: StateStoreId, keySchema: StructType, valueSchema: StructType,
                    keyIndexOrdinal: Option[Int], storeConfs: StateStoreConf,
                    hadoopConf: Configuration): Unit = {
    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.storeConf = storeConfs
    LocalDynamoDbTestContext.createTablesIfNotExist()
  }

  override def stateStoreId: StateStoreId = stateStoreId_

  override def close(): Unit = {
    LocalDynamoDb.dynamoDbLocal.shutdown()
  }

  override def getStore(version: Long): StateStore = {
    val snapshotGroupToRestore = getSnapshotGroup(version, storeConf.minDeltasForSnapshot)
    val newVersion = version + 1
    if (newVersion > 0 && StatesWithVersions.isEmpty) {
      println(s"Got ${snapshotGroupToRestore} <<<<< ${version}")

      // It will be called every time simply because I want the proof that restoring state
      // will work.
      logDebug(s"Restoring map for ${snapshotGroupToRestore}")
      println(s"Restoring map for ${snapshotGroupToRestore}")
      val restoredMap = LocalDynamoDbStateStoreRecovery.recoverState(
        snapshotGroup = snapshotGroupToRestore,
        partition = stateStoreId_.partitionId,
        maxDeltaVersion = newVersion,
        maxSuffixNumber = DynamoDbStateStoreParams.SnapshotMaxSuffixes,
        dynamoDbProxy = new DynamoDbProxy(LocalDynamoDb.dynamoDbConnector))
      restoredMap.foreach(pair => StatesWithVersions.put(pair._1, pair._2))
      logDebug(s"Got restoredMap=${restoredMap}")
      println(s"Got restoredMap=${restoredMap}")
    }

    new LocalDynamoDbStateStore(id = stateStoreId_,
      keySchema = keySchema, valueSchema = valueSchema,
      version = newVersion, dynamoDb = LocalDynamoDb.dynamoDbConnector)
  }

  // Dividend and divisor are both integers, so the result will be also an integer (no need for rounding)
  private def getSnapshotGroup(stateStoreVersion: Long, minDeltasForSnapshot: Int) = stateStoreVersion / minDeltasForSnapshot

  class LocalDynamoDbStateStore(override val id: StateStoreId,
                                keySchema: StructType,
                                valueSchema: StructType,
                                override val version: Long,
                                dynamoDb: DynamoDB) extends StateStore {
    private var calledGets = 0
    private var calledFlushes = 0
    private val dynamoDbProxy = new DynamoDbProxy(dynamoDb)
    private var isCommitted = false
    private val deletedStates = new mutable.ListBuffer[String]()

    private lazy val snapshotGroup = getSnapshotGroup(version, storeConf.minDeltasForSnapshot)
    private lazy val isFirstSnapshotGroup = version % storeConf.minDeltasForSnapshot == 0

    override def get(inputStateKey: UnsafeRow): UnsafeRow = {
      calledGets += 1
      val lastVersionOption = StatesWithVersions.getOrElse(inputStateKey.toString, SnapshotGroupForKey())
        .deltaVersions.lastOption
      lastVersionOption.map(lastVersion => {
        val state = dynamoDb.getTable(StateStoreTable)
          .getItem(new PrimaryKey(MappingStateStore.PartitionKey, s"${inputStateKey.toString}_${lastVersion}"))
        val (_, value) = UnsafeRowConverter.getStateFromCompressedState(
          state.getBinary(MappingStateStore.StateData), keySchema, valueSchema)
        value
      }).getOrElse({
        null
      })
    }

    override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
      UpdatesList.offer(StateStoreChange.fromUpdateAction(key.copy(), Some(value.copy()), false))
      val stateVersions = StatesWithVersions.getOrElseUpdate(key.toString, SnapshotGroupForKey())
      val updatedStateVersion = stateVersions.addVersion(version)
      StatesWithVersions.put(key.toString, updatedStateVersion)
      flushChangesForThreshold()
    }

    override def remove(key: UnsafeRow): Unit = {
      UpdatesList.offer(StateStoreChange.fromUpdateAction(key.copy(), None, true))
      // We'll remove the state that for sure exists in the state store - otherwise, fail fast because it's not
      // normal to remove a not existent state
      val stateVersions = StatesWithVersions(key.toString)
      val deletedStateVersion = stateVersions.delete(version)
      StatesWithVersions.put(key.toString, deletedStateVersion)
      deletedStates.append(key.toString)
      flushChangesForThreshold()
    }

    private def flushChangesForThreshold() = {
      if (UpdatesList.size == DynamoDbStateStoreParams.FlushThreshold) {
        flushChanges()
      }
    }

    private def flushChanges(): Unit = {
      val itemsToWrite = (0 until DynamoDbStateStoreParams.FlushThreshold).map(_ => UpdatesList.poll())
        .filter(item => item != null)
      if (itemsToWrite.nonEmpty) {
        calledFlushes += 1
        dynamoDbProxy.writeItems[StateStoreChange](StateStoreTable, itemsToWrite,
          (stateStoreItem => stateStoreItem.toItem(version)))

        dynamoDbProxy.writeItems[StateStoreChange](SnapshotTable, itemsToWrite,
          (stateStoreItem => createItemForSnapshotGroupState(stateStoreItem.key))
        )
      }
    }
    override def commit(): Long = {
      flushChanges()
      // I got the reason of keeping deletedStates apart => it'll be useful for abort
      // If we abort, we cancel all changed made so far, so we can simply change the versions
      deletedStates.foreach(stateKey => {
        StatesWithVersions.remove(stateKey)
      })
      if (isFirstSnapshotGroup) {
        // It simulates the real snapshot from HDFS-backed data source
        // For the last version in given snapshot group we save all state in the snapshot table
        // to retrieve them in the next group
        val mappingFunction: (String) => Item = (stateKey => {
          createItemForSnapshotGroupState(stateKey)
        })
        dynamoDbProxy.writeItems[String](SnapshotTable, StatesWithVersions.keys.toSeq, mappingFunction)
      }
      isCommitted = true
      version
    }

    override def abort(): Unit = {
      val modifiedStates = StatesWithVersions.filter {
        case (_, stateVersions) => stateVersions.lastVersion == version
      }
      val statesToWrite = modifiedStates.grouped(DynamoDbStateStoreParams.FlushThreshold)
      statesToWrite.foreach(states => {
        abortAlreadyExistentSnapshots(states.toMap)
        abortNewSnapshots(states)
        abortDeltaChanges(states)
      })
      deletedStates.clear()
    }
    private def abortAlreadyExistentSnapshots(states: collection.Map[String, SnapshotGroupForKey]) = {
      val notEmptyStates = states
        .map { case (key, versions) => (key, versions.abort) }
        .filter { case (_, versions) => versions.deltaVersions.nonEmpty }.keys
      val mappingFunction: (String) => Item = (stateKey => {
        createItemForSnapshotGroupState(stateKey)
      })
      dynamoDbProxy.writeItems[String](SnapshotTable, notEmptyStates.toSeq, mappingFunction)
    }
    private def abortNewSnapshots(states: collection.Map[String, SnapshotGroupForKey]) = {
      val emptyStates = states
        .map { case (key, versions) => (key, versions.abort) }
        .filter { case (_, versions) => versions.deltaVersions.isEmpty }
      val snapshotGroupsToDelete = emptyStates.keys
        .map(key => (snapshotPartitionKeyWithoutSuffix(snapshotGroup, stateStoreId.partitionId, key), key))
      val itemHandler: (TableWriteItems, (String, String)) => Unit = (tableWriteItems, key) => {
        tableWriteItems.addHashAndRangePrimaryKeyToDelete(MappingSnapshot.PartitionKey, key._1,
          MappingSnapshot.StateKey, key._2)
      }
      dynamoDbProxy.deleteItems[(String, String)](SnapshotTable, snapshotGroupsToDelete.toSeq, itemHandler)
    }
    private def abortDeltaChanges(states: collection.Map[String, SnapshotGroupForKey]) = {
      val partitionKeysToDelete = states.keys.map(stateKey => s"${stateKey}_${version}").toSeq
      val itemHandler: (TableWriteItems, String) => Unit = (tableWriteItems, key) => {
        tableWriteItems.addHashOnlyPrimaryKeyToDelete(MappingStateStore.PartitionKey, key)
      }
      dynamoDbProxy.deleteItems[String](StateStoreTable, partitionKeysToDelete, itemHandler)
    }

    private def createItemForSnapshotGroupState(stateKey: String): Item = {
      new Item()
        .withPrimaryKey(MappingSnapshot.PartitionKey,
          snapshotPartitionKeyWithoutSuffix(snapshotGroup, stateStoreId.partitionId, stateKey),
          MappingSnapshot.StateKey, stateKey
        )
        .withString(MappingSnapshot.DeltaVersions,
          DynamoDbStateStoreParams.deltaVersionsOutput(StatesWithVersions(stateKey).deltaVersions))
    }

    override def iterator(): Iterator[UnsafeRowPair] = new LocalDynamoDbStateStoreIterator(
      statesWithVersion = StatesWithVersions, keySchema = keySchema, valueSchema = valueSchema,
      dynamoDbProxy = dynamoDbProxy
    )

    override def metrics: StateStoreMetrics = StateStoreMetrics(
      numKeys = StatesWithVersions.size,
      memoryUsedBytes = SizeEstimator.estimate(StatesWithVersions),
      customMetrics = Map(
        customMetricFlushes -> calledFlushes,
        customMetricGets -> calledGets
      )
    )

    override def hasCommitted: Boolean = isCommitted
  }

  // If you want to use custom metrics, you must override this method
  // Otherwise, you will get this exception:
  //  Caused by: java.util.NoSuchElementException: key not found: FlushCalls
  //	  at scala.collection.MapLike$class.default(MapLike.scala:228)
  //	  at scala.collection.AbstractMap.default(Map.scala:59)
  //	  at scala.collection.MapLike$class.apply(MapLike.scala:141)
  //	  at scala.collection.AbstractMap.apply(Map.scala:59)
  //	  at org.apache.spark.sql.execution.SparkPlan.longMetric(SparkPlan.scala:91)
  //	  at org.apache.spark.sql.execution.streaming.StateStoreWriter$$anonfun$setStoreMetrics$1.apply(statefulOperators.scala:119)
  //	  at org.apache.spark.sql.execution.streaming.StateStoreWriter$$anonfun$setStoreMetrics$1.apply(statefulOperators.scala:118)
  //	  at scala.collection.immutable.Map$Map2.foreach(Map.scala:137)
  //	  at org.apache.spark.sql.execution.streaming.StateStoreWriter$class.setStoreMetrics(statefulOperators.scala:118)
  //	  at org.apache.spark.sql.execution.streaming.FlatMapGroupsWithStateExec.setStoreMetrics(FlatMapGroupsWithStateExec.scala:44)
  //	  at org.apache.spark.sql.execution.streaming.FlatMapGroupsWithStateExec$$anonfun$doExecute$1$$anonfun$apply$1.apply$mcV$sp(FlatMapGroupsWithStateExec.scala:136)
  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = {
    customMetricFlushes :: customMetricGets ::
      Nil
  }

}

object LocalDynamoDb {

  lazy val dynamoDbLocal: AmazonDynamoDB = {
    System.setProperty("sqlite4java.library.path", "native-libs")
    DynamoDBEmbedded.create().amazonDynamoDB()
  }

  lazy val dynamoDbConnector = new DynamoDB(dynamoDbLocal)
}



case class SnapshotGroupForKey(deltaVersions: Seq[Long] = Seq.empty, deltaDeleteVersion: Option[Long] = None) {

  def addVersion(delta: Long): SnapshotGroupForKey = {
    this.copy(deltaVersions = deltaVersions :+ delta)
  }

  def delete(delta: Long): SnapshotGroupForKey = {
    addVersion(delta).copy(deltaDeleteVersion = Some(delta))
  }

  def abort: SnapshotGroupForKey = {
    this.copy(deltaVersions = deltaVersions.dropRight(1), deltaDeleteVersion = None)
  }

  lazy val lastVersion = deltaVersions.last
}
