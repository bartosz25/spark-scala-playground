package com.waitingforcode.sparksummit2019.customstatestore

import com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, TableKeysAndAttributes, TableWriteItems}
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ComparisonOperator, Condition, QueryRequest}

class DynamoDbProxy(dynamoDb: DynamoDB) {

  def writeItems[T](targetTable: String, items: Seq[T], mappingFunction: T => Item) = {
    val itemsToWrite = items.map(item => mappingFunction(item))
    val writeRequest = new BatchWriteItemSpec()
      .withTableWriteItems(new TableWriteItems(targetTable)
        .withItemsToPut(itemsToWrite: _*))
    val result = dynamoDb.batchWriteItem(writeRequest)
    var unprocessedItems = result.getUnprocessedItems
    while (unprocessedItems.containsKey(targetTable)) {
      val retryResult = dynamoDb.batchWriteItemUnprocessed(unprocessedItems)
      unprocessedItems = retryResult.getUnprocessedItems
    }
  }

  def deleteItems[T](targetTable: String, partitionKeysToDelete: Seq[T],
                     itemsHandler: (TableWriteItems, T) => Unit) = {
    val tableWriteItems = new TableWriteItems(targetTable)
    partitionKeysToDelete.foreach(items => {
      itemsHandler(tableWriteItems, items)
    })
    val writeRequest = new BatchWriteItemSpec()
      .withTableWriteItems(tableWriteItems)
    val result = dynamoDb.batchWriteItem(writeRequest)
    var unprocessedItems = result.getUnprocessedItems
    while (unprocessedItems.containsKey(targetTable)) {
      val retryResult = dynamoDb.batchWriteItemUnprocessed(unprocessedItems)
      unprocessedItems = retryResult.getUnprocessedItems
    }
  }

  def readItems[T](sourceTable: String, partitionKeysToGet: Seq[String], mappingFunction: Item => T,
                   partitionKey: String, attributesToGet: Seq[String]): Seq[T] = {
    val batchGetSpec = new TableKeysAndAttributes(sourceTable)
      .addHashOnlyPrimaryKeys(partitionKey, partitionKeysToGet: _*)
      .withAttributeNames(attributesToGet: _*)
    val batchResult = dynamoDb.batchGetItem(batchGetSpec)
    import scala.collection.JavaConverters._
    var states = batchResult.getTableItems.getOrDefault(sourceTable, List.empty[Item].asJava)
      .asScala.map(item => mappingFunction(item))
    var unprocessedKeys = batchResult.getUnprocessedKeys
    while (!unprocessedKeys.isEmpty) {
      val result = dynamoDb.batchGetItemUnprocessed(unprocessedKeys)
      val newStates = batchResult.getTableItems.getOrDefault(sourceTable, List.empty[Item].asJava)
        .asScala.map(item => mappingFunction(item))
      states = states ++ newStates
      unprocessedKeys = result.getUnprocessedKeys
    }
    states
  }

  def queryAllItemsForPartitionKey[T](sourceTable: String, partitionKey: String, partitionKeyToGet: String,
                                   projectionAttributes: Seq[String], mapper: (Map[String, AttributeValue]) => T): Seq[T] = {
    import scala.collection.JavaConverters._

    val querySpec = new QueryRequest()
      .withTableName(sourceTable)
      .withKeyConditions(Map(partitionKey -> new Condition()
        .withComparisonOperator(ComparisonOperator.EQ.toString)
        .withAttributeValueList(new AttributeValue().withS(partitionKeyToGet))).asJava)
      .withAttributesToGet(projectionAttributes: _*)

    var result = LocalDynamoDb.dynamoDbLocal.query(querySpec)
    var outputResults = Seq[T]()
    do {
      outputResults = outputResults ++ result.getItems.asScala.map(attributes => mapper(attributes.asScala.toMap))
      result = LocalDynamoDb.dynamoDbLocal.query(querySpec.withExclusiveStartKey(result.getLastEvaluatedKey))
    } while (result.getLastEvaluatedKey != null)
    outputResults
  }

}
