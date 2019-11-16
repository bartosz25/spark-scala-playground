package com.waitingforcode.sparksummit2019.customstatestore

import com.waitingforcode.sparksummit2019.customstatestore.DynamoDbStateStoreParams.MappingSnapshot.{DeleteDeltaVersion, DeltaVersions, PartitionKey, StateKey}
import com.waitingforcode.sparksummit2019.customstatestore.DynamoDbStateStoreParams.SnapshotTable

object LocalDynamoDbStateStoreRecovery {

  def recoverState(snapshotGroup: Long, partition: Int, maxDeltaVersion: Long, maxSuffixNumber: Int,
                   dynamoDbProxy: DynamoDbProxy): Map[String, SnapshotGroupForKey] = {
    val partitionKeys = (0 until maxSuffixNumber).map(suffix => {
      DynamoDbStateStoreParams.snapshotPartitionKey(snapshotGroup, partition, suffix)
    })
    partitionKeys.flatMap(partitionKey => {
      dynamoDbProxy.queryAllItemsForPartitionKey(SnapshotTable, PartitionKey, partitionKey, Seq(StateKey, DeltaVersions,
        DeleteDeltaVersion),
        (attributes) => {
          val versionsToKeep = DynamoDbStateStoreParams.deltaVersionFromString(attributes(DeltaVersions).getS)
            .filter(deltaVersion => deltaVersion <= maxDeltaVersion)
          val deletedInVersion = Option(attributes(DeleteDeltaVersion).getS)

          val snapshotGroupForKey = deletedInVersion.map(deleteVersion => {
            if (deleteVersion.toLong == versionsToKeep.last) {
              None
            } else {
              Some(SnapshotGroupForKey(versionsToKeep))
            }
          }).getOrElse(Some(SnapshotGroupForKey(versionsToKeep)))
          (attributes(StateKey).getS, snapshotGroupForKey)
        }
      )
    }).filter {
      case (_, snapshotGroupForKey) => snapshotGroupForKey.isDefined
    }
    .map {
      case (key, snapshotGroupForKey) => (key, snapshotGroupForKey.get)
    }
    .toMap
  }

}
