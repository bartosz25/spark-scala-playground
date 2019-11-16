package com.waitingforcode.sparksummit2019.customstatestore

import com.amazonaws.services.dynamodbv2.document._
import com.waitingforcode.sparksummit2019.customstatestore.DynamoDbStateStoreParams.MappingStateStore
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

import scala.util.Random


case class StateStoreChange(key: String, gzipCompressedUnsafeRow: Array[Byte],
                            expirationTimeMillis: Option[Long] = None, isDeleted: Option[Boolean] = None) {
  def toItem(queryVersion: Long): Item = {
    val item = new Item()
      .withPrimaryKey(MappingStateStore.PartitionKey, MappingStateStore.generatePartitionKey(key, queryVersion))
      .withBinary(MappingStateStore.StateData, gzipCompressedUnsafeRow)
    isDeleted.filter(stateDeleted => stateDeleted).foreach(_ => item.withBoolean(MappingStateStore.IsDeleteFlag, true))
    item
  }
}
object StateStoreChange {
  def fromItem(item: Item): StateStoreChange = {
    val expirationTime = if (item.isPresent(MappingStateStore.ExpirationTimeMs)) {
      Some(item.getLong(MappingStateStore.ExpirationTimeMs))
    } else {
      None
    }
    StateStoreChange(MappingStateStore.restorePartitionKey(item),
      item.getBinary(MappingStateStore.StateData), expirationTime)
  }

  def fromUpdateAction(key: UnsafeRow, value: Option[UnsafeRow], isDelete: Boolean): StateStoreChange = {
    // TODO: handle expiration time (? check if needed)
    StateStoreChange(key.toString, UnsafeRowConverter.compressKeyAndValue(key, value, isDelete),
      None, Some(isDelete))
  }

}

object DynamoDbStateStoreParams {

  val FlushThreshold = 25
  val MaxItemsInQuery = 100

  val StateStoreTable = "state-store"
  object MappingStateStore {
    val PartitionKey = "StateKey_QueryVersion"
    val StateData = "State"
    val ExpirationTimeMs = "ExpirationTimeMs"
    val IsDeleteFlag = "IsDeleted"

    def generatePartitionKey(key: String, queryVersion: Long): String = s"${key}_${queryVersion}"
    def restorePartitionKey(item: Item) = {
      val keyWithQueryVersion = item.getString(MappingStateStore.PartitionKey).split("_")
      keyWithQueryVersion(0)
    }
  }

  val SnapshotTable = "snapshot"
  object MappingSnapshot {
    val PartitionKey = "SnapshotGroup_Partition"
    val StateKey = "StateKey"
    val DeltaVersions = "DeltaVersions"
    val DeleteDeltaVersion = "DeleteDeltaVersion"
  }
  // Partition keys in snapshot groups table won't be correctly distributed. To improve
  // the distribution, thus the throughput, I will add a suffix computed from the sort key
  // This variable represents the max number of available suffixes. For that specific case it's an
  // arbitrary number.
  // Doc: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-sharding.html
  val SnapshotMaxSuffixes = 200
  def snapshotPartitionKeyWithoutSuffix(snapshotGroup: Long, partitionId: Int, stateKey: String): String = {
    snapshotPartitionKey(snapshotGroup, partitionId, getSuffixFromSortKey(stateKey))
  }
  def snapshotPartitionKey(snapshotGroup: Long, partitionId: Int, suffix: Int): String = {
    s"${snapshotGroup}_${partitionId}#${suffix}"
  }
  def deltaVersionsOutput(versions: Seq[Long]): String = versions.mkString(",")
  def deltaVersionFromString(deltaVersions: String) = deltaVersions.split(",")
    .map(snapshotVersion => snapshotVersion.toLong)
  private def getSuffixFromSortKey(sortKey: String): Int = {
    val randomGenerator = new Random(sortKey.hashCode)
    randomGenerator.nextInt(SnapshotMaxSuffixes)
  }

}
