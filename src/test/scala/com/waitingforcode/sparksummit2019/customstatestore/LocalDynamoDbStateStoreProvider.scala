package com.waitingforcode.sparksummit2019.customstatestore

import java.util.concurrent.ConcurrentLinkedQueue

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded
import com.waitingforcode.sparksummit2019.customstatestore.DynamoDbStateStoreParams._
import org.apache.hadoop.conf.Configuration
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
class LocalDynamoDbStateStoreProvider extends StateStoreProvider {

  // I use the same values as in HDFSBackedStateStoreProvider
  // Except for: hadoopConf and numberOfVersionsToRetainInMemory because the internal logic won't use them
  @volatile private var stateStoreId_ : StateStoreId = _
  @volatile private var keySchema: StructType = _
  @volatile private var valueSchema: StructType = _
  @volatile private var storeConf: StateStoreConf = _

  // It would be better to have these fields in an object. The problem is that the state store is partition-based
  // and the object is located on a single JVM, so it'll be shared across partitions.
  private lazy val UpdatesList = new ConcurrentLinkedQueue[StateStoreChange]()

  private lazy val StatesWithVersions = new mutable.HashMap[String, mutable.Buffer[Long]]()

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
    if (version > 0) {
      // TODO: it's probably called every time, even when the data is present in StateWithVersions
      // TODO: confirm that and if it's the case, we can use that as the recovery proof :)

      // TODO: change to > 1
      // TODO: see later how to fix the error of "The number of conditions on the keys is invalid"
      // TODO: use https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html for that
      val restoredMap = LocalDynamoDbStateStoreRecovery.recoverState(version, stateStoreId_.partitionId,
        DynamoDbStateStoreParams.SnapshotMaxSuffixes, new DynamoDbProxy(LocalDynamoDb.dynamoDbConnector))
      restoredMap.foreach(pair => StatesWithVersions.put(pair._1, pair._2))
    }


    new LocalDynamoDbStateStore(id = stateStoreId_,
      keySchema = keySchema, valueSchema = valueSchema,
      version = version, dynamoDb = LocalDynamoDb.dynamoDbConnector)
  }

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

    private val newVersion = version + 1

    // Dividend and divisor are both integers, so the result will be also an integer (no need for rounding)
    private lazy val snapshotGroup = newVersion / storeConf.minDeltasForSnapshot

    override def get(inputStateKey: UnsafeRow): UnsafeRow = {
      calledGets += 1
      val lastVersionOption = StatesWithVersions.getOrElse(inputStateKey.toString, new mutable.ListBuffer[Long]())
        .lastOption
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
      val stateVersions = StatesWithVersions.getOrElseUpdate(key.toString, new mutable.ListBuffer[Long]())
      stateVersions.append(newVersion)
      StatesWithVersions.put(key.toString, stateVersions)
      flushChangesForThreshold()
    }

    override def remove(key: UnsafeRow): Unit = {
      UpdatesList.offer(StateStoreChange.fromUpdateAction(key.copy(), None, true))
      StatesWithVersions(key.toString).append(newVersion)
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
          (stateStoreItem => stateStoreItem.toItem(newVersion)))

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
        println(s"#commit remove state for ${stateKey}")
        StatesWithVersions.remove(stateKey)
      })
      isCommitted = true
      println(s"#commit StatesWithVersions after=${StatesWithVersions}")
      newVersion
    }

    override def abort(): Unit = {
      println("#abort Aborting StateStore")
      val modifiedStates = StatesWithVersions.filter {
        case (_, versions) => versions.last == newVersion
      }
      val statesToWrite = modifiedStates.grouped(DynamoDbStateStoreParams.FlushThreshold)
      statesToWrite.foreach(states => {
        states.values.foreach(versions => versions.remove(versions.length))
        abortAlreadyExistentSnapshots(states.toMap)
        abortNewSnapshots(states)
        abortDeltaChanges(states.toMap)
      })
      deletedStates.clear()
    }
    private def abortAlreadyExistentSnapshots(states: collection.Map[String, mutable.Buffer[Long]]) = {
      val notEmptyStates = states.filter { case (_, versions) => versions.nonEmpty }.keys.toSeq
      val mappingFunction: (String) => Item = (stateKey => {
        createItemForSnapshotGroupState(stateKey)
      })
      dynamoDbProxy.writeItems[String](SnapshotTable, notEmptyStates, mappingFunction)
    }
    private def abortNewSnapshots(states: collection.Map[String, mutable.Buffer[Long]]) = {
      val emptyStates = states.filter { case (_, versions) => versions.isEmpty }
      val snapshotGroupsToDelete = emptyStates.keys
        .map(key => (snapshotPartitionKeyWithoutSuffix(snapshotGroup, stateStoreId.partitionId, key), key))
      val itemHandler: (TableWriteItems, (String, String)) => Unit = (tableWriteItems, key) => {
        tableWriteItems.addHashAndRangePrimaryKeyToDelete(MappingSnapshot.PartitionKey, key._1,
          MappingSnapshot.StateKey, key._2)
      }
      dynamoDbProxy.deleteItems[(String, String)](SnapshotTable, snapshotGroupsToDelete.toSeq, itemHandler)
    }
    private def abortDeltaChanges(states: collection.Map[String, mutable.Buffer[Long]]) = {
      val partitionKeysToDelete = states.keys.map(stateKey => s"${stateKey}_${newVersion}").toSeq
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
          DynamoDbStateStoreParams.deltaVersionsOutput(StatesWithVersions(stateKey)))
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