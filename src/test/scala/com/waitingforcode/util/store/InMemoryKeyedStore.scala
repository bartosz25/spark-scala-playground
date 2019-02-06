package com.waitingforcode.util.store

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
/**
  * In memory data store used in tests assertions.
  */
object InMemoryKeyedStore {

  private val Data = new mutable.HashMap[String, mutable.ListBuffer[String]]()

  def addValue(key: String, value: String) = {
    Data.synchronized {
      val values = Data.getOrElse(key, new mutable.ListBuffer[String]())
      values.append(value)
      Data.put(key, values)
    }
  }

  def getValues(key: String): mutable.ListBuffer[String] = Data.getOrElse(key, ListBuffer.empty)

  def allValues = Data
}
