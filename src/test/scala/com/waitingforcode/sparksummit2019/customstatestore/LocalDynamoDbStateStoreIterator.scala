package com.waitingforcode.sparksummit2019.customstatestore

import com.waitingforcode.sparksummit2019.customstatestore.DynamoDbStateStoreParams.{MappingStateStore, StateStoreTable}
import org.apache.spark.sql.execution.streaming.state.UnsafeRowPair
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

class LocalDynamoDbStateStoreIterator(statesWithVersion: mutable.Map[String, SnapshotGroupForKey],
                                      keySchema: StructType, valueSchema: StructType,
                                      dynamoDbProxy: DynamoDbProxy) extends Iterator[UnsafeRowPair] {
  private val unsafeRowPair = new UnsafeRowPair()
  private val statesToReturn = new mutable.Queue[StateStoreChange]()
  private val statesFetchThreshold = 3
  private val stateKeys = statesWithVersion.keysIterator

  override def hasNext: Boolean = {
    if (statesToReturn.size <= statesFetchThreshold) {
      val retrievedStates = stateKeys.take(500).grouped(DynamoDbStateStoreParams.MaxItemsInQuery)
        .flatMap(keys => fetchStateFromDynamoDb(keys)).toSeq
      statesToReturn.enqueue(retrievedStates: _*)
    }
    stateKeys.hasNext || statesToReturn.size > 0
  }

  override def next(): UnsafeRowPair = {
    val state = statesToReturn.dequeue()
    val (key, value) = UnsafeRowConverter.getStateFromCompressedState(state.gzipCompressedUnsafeRow,
      keySchema, valueSchema)
    unsafeRowPair.withRows(key, value)
  }

  private def fetchStateFromDynamoDb(groupedKeys: Seq[String]): Seq[StateStoreChange] = {
    val partitionKeysToGet = groupedKeys.map(key => {
      MappingStateStore.generatePartitionKey(key, statesWithVersion(key).lastVersion)
    })
    val states = dynamoDbProxy.readItems(StateStoreTable, partitionKeysToGet, StateStoreChange.fromItem _,
      MappingStateStore.PartitionKey, Seq(MappingStateStore.PartitionKey, MappingStateStore.StateData))
    states
  }

}
