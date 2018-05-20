package com.waitingforcode.util

import com.waitingforcode.util.store.InMemoryKeyedStore
import org.apache.spark.sql.ForeachWriter

class InMemoryStoreWriter[T](key: String, rowExtractor: (T) => String) extends ForeachWriter[T] {
  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(row: T): Unit = {
    InMemoryKeyedStore.addValue(key, rowExtractor(row))
  }

  override def close(errorOrNull: Throwable): Unit = {}
}
