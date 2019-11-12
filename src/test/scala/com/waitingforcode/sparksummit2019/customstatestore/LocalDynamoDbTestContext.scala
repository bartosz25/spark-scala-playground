package com.waitingforcode.sparksummit2019.customstatestore

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils

object LocalDynamoDbTestContext {

  def createTablesIfNotExist(): Unit = {
    val createTableQueries = Seq(
      (DynamoDbStateStoreParams.StateStoreTable, (DynamoDbStateStoreParams.MappingStateStore.PartitionKey, "S"), None),
      (DynamoDbStateStoreParams.SnapshotTable, (DynamoDbStateStoreParams.MappingSnapshot.PartitionKey, "S"),
        Some(DynamoDbStateStoreParams.MappingSnapshot.StateKey, "S"))
    )
    for ((table, partitionKeySpec, rangeKey) <- createTableQueries) {
      val partitionKey = new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName(partitionKeySpec._1)
      val keySchemaElements = new java.util.ArrayList[KeySchemaElement]()
      val attributeDefinitions = new java.util.ArrayList[AttributeDefinition]()
      attributeDefinitions.add(new AttributeDefinition(partitionKeySpec._1, partitionKeySpec._2))
      keySchemaElements.add(partitionKey)
      rangeKey.foreach {
        case (rangeKey, rangeKeyType) => {
          keySchemaElements.add(new KeySchemaElement().withKeyType(KeyType.RANGE).withAttributeName(rangeKey))
          attributeDefinitions.add(new AttributeDefinition(rangeKey, rangeKeyType))
        }
      }
      val testTableRequest = new CreateTableRequest(table, keySchemaElements)
      testTableRequest.withAttributeDefinitions(attributeDefinitions).withProvisionedThroughput(new ProvisionedThroughput(100L, 100L))
      TableUtils.createTableIfNotExists(LocalDynamoDb.dynamoDbLocal, testTableRequest)
    }
  }

  def shutdownEmbeddedDynamoDb(): Unit = {
    LocalDynamoDb.dynamoDbLocal.shutdown()
  }

}

