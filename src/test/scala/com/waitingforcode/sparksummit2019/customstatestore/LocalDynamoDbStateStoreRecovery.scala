package com.waitingforcode.sparksummit2019.customstatestore

import com.waitingforcode.sparksummit2019.customstatestore.DynamoDbStateStoreParams.MappingSnapshot.{StateKey, DeltaVersions, PartitionKey}
import com.waitingforcode.sparksummit2019.customstatestore.DynamoDbStateStoreParams.SnapshotTable

object LocalDynamoDbStateStoreRecovery {

  def recoverState(version: Long, partition: Int, maxVersions: Int, dynamoDbProxy: DynamoDbProxy) = {
    val basicPartitionKey = s"${version}_${partition}"
    val partitionKeys = (0 until maxVersions).map(suffix => {
      DynamoDbStateStoreParams.snapshotPartitionKey(version, partition, suffix)
      s"${basicPartitionKey}#${suffix}"
    })

    partitionKeys.flatMap(partitionKey => {
      dynamoDbProxy.queryAllItemsForPartitionKey(SnapshotTable, PartitionKey, partitionKey, Seq(PartitionKey, StateKey),
        (attributes) => {
          val versionsToKeep = DynamoDbStateStoreParams.deltaVersionFromString(attributes(DeltaVersions).getS)
            .filter(snapshotVersion => snapshotVersion < version)

          (attributes(PartitionKey).getS, versionsToKeep.toBuffer)
        }
      )
    }).toMap
  }

}
