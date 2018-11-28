package org.apache.spark

import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.io.ChunkedByteBuffer

import scala.reflect.ClassTag

object SparkMemoryDecorations {

  def decorateAcquireStorageMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Unit = {
    println(s"[MemoryManager] Acquiring ${numBytes} bytes for ${blockId} in ${memoryMode} memory mode")
  }

  def decorateAcquireUnrollMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Unit = {
    println(s"[MemoryManager]  Unrolling ${numBytes} bytes for ${blockId} in ${memoryMode} memory mode")
  }

  def decoratePutBlockData(blockId: BlockId, data: ManagedBuffer, level: StorageLevel, classTag: ClassTag[_]): Unit = {
    println(s"[BlockManager] putBlockData=${data}")
  }

  def decoratePutIterator(blockId: BlockId, values: Iterator[_], level: StorageLevel, tellMaster: Boolean): Unit = {
    println(s"[BlockManager] putIterator=${values.getClass}")
  }

  def decoratePutBytes(blockId: BlockId, bytes: ChunkedByteBuffer, level: StorageLevel, tellMaster: Boolean): Unit = {
    println(s"[BlockManager] putBytes=${bytes}")
  }

  def decoratePutSingle(blockId: BlockId, value: Any, level: StorageLevel, tellMaster: Boolean) = {
    val valueToPrint = value match {
      case array: Array[_] => array.mkString("," )
      case data => data
    }
    println(s"[BlockManager] putSingle=${valueToPrint}")
  }

  def decorateDoPut(blockId: BlockId, level: StorageLevel,classTag: ClassTag[_], tellMaster: Boolean, keepReadLock: Boolean) = {
    println(s"[BlockManager] Putting block ${blockId}")
  }

  def decorateAllocatePage(taskMemoryManager: TaskMemoryManager, size: Long, memoryConsumer: MemoryConsumer) = {
    println(s"[TaskMemoryManager] Allocating page ${size} bytes for a task")
    taskMemoryManager.showMemoryUsage()
  }

  def decorateCleanUpAllAllocatedMemory(taskMemoryManager: TaskMemoryManager): Unit = {
    println("[TaskMemoryManager] Freeing task resources")
  }

  def decorateAcquireExecutionMemory(size: Long, consumer: MemoryConsumer): Unit = {
    println(s"[TaskMemoryManager] Trying to acquire ${size} bytes of execution memory for ${consumer.getClass} consumer")
  }

  def decorateReleaseExecutionMemory(size: Long, consumer: MemoryConsumer) = {
    println(s"[TaskMemoryManager] Releasing execution memory ${size} bytes from ${consumer.getClass} consumer")
  }

  def decorateAllocatePage(size: Long, consumer: MemoryConsumer): Unit = {
    println(s"[TaskMemoryManager] Allocating ${size} bytes page for ${consumer.getClass} consumer")
  }

  def decorateGrow(neededSize: Int, row: UnsafeRow) = {
    println(s"[BufferHolder] Need ${neededSize} bytes for row ${row.getSizeInBytes}")
  }

  def decorateAcquireMemory(size: Long, memoryConsumer: MemoryConsumer) = {
    println(s"[MemoryConsumer] Acquiring ${size} bytes for MemoryConsumer of ${memoryConsumer.getClass}")
  }

  def decoratePutBytesMemoryStore(blockId: BlockId, size: Long) = {
    println(s"[MemoryStore] Putting block ${blockId} of ${size} bytes into MemoryStore")
  }

  def decoratePuIteratorAsValuesMemoryStore(blockId: BlockId) = {
    println(s"[MemoryStore] Putting value iterator block ${blockId} into MemoryStore")
  }

  def decoratePutIteratorAsBytesMemoryStore(blockId: BlockId) = {
    println(s"[MemoryStore] Putting bytes iterator block ${blockId} into MemoryStore")
  }

  def decorateRemoveMemoryStore(blockId: BlockId) = {
    println(s"[MemoryStore] Removing block ${blockId} from MemoryStore")
  }

  def decorateClearMemoryStore() = {
    println(s"[MemoryStore] Clearing memory store")
  }

  def decorateEvictBlocksToFreeSpaceMemoryStore(blockId: Option[BlockId], size: Long) = {
    println(s"[MemoryStore] Evicting ${blockId} from MemoryStore to gain ${size} bytes")
  }

  def decorateReserveUnrollMemoryForThisTask(blockId: BlockId, memory: Long, memoryMode: MemoryMode) = {
    println(s"[MemoryStore] Unrolling reserve ${blockId}, ${memory} bytes of mode ${memoryMode}")
  }

  def decorateUnsafeRowConstructor(unsafeRow: UnsafeRow): Unit = {
    println(s"[UnsafeRow] Creating new UnsafeRow with ${unsafeRow.numFields()} fields (${unsafeRow})")
  }

  def decorateCompute(split: Partition): Unit = {
    println(s"[HadoopRDD] Computing partition ${split}")
  }

  def decorateInsertAppendOnlyMap(key: Any) = {
    println(s"[ExternalAppendOnlyMap] Inserting ${key}")
  }

}
