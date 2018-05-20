package com.waitingforcode.util

import org.apache.spark.sql.ForeachWriter

class NoopForeachWriter[T] extends ForeachWriter[T] {
  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: T): Unit = {}

  override def close(errorOrNull: Throwable): Unit = {}
}
