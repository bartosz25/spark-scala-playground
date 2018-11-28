package com.waitingforcode.memoryclasses

import com.waitingforcode.util.javaassist.SparkMemoryClassesDecorators
import org.apache.spark.sql.SparkSession

object DatasetPipeline {

  SparkMemoryClassesDecorators.decoratePuts().toClass
  SparkMemoryClassesDecorators.decorateTaskMemoryManager().toClass
  SparkMemoryClassesDecorators.decorateBufferHolder().toClass
  SparkMemoryClassesDecorators.decorateMemoryConsumer().toClass
  SparkMemoryClassesDecorators.decorateMemoryStore().toClass
  SparkMemoryClassesDecorators.decorateUnsafeRow.toClass
  SparkMemoryClassesDecorators.decorateHadoopRDD.toClass
  SparkMemoryClassesDecorators.decorateExternalAppendOnlyMap.toClass
  SparkMemoryClassesDecorators.decorateAcquireStorageMemory(
    "org.apache.spark.memory.StaticMemoryManager", "acquireStorageMemory").toClass
  SparkMemoryClassesDecorators.decorateAcquireStorageMemory(
    "org.apache.spark.memory.UnifiedMemoryManager", "acquireStorageMemory").toClass

  createTestFilesIfMissing(100000, 3)


  def main(args: Array[String]): Unit = {
    println("Waiting 10 seconds before getting connection")
    Thread.sleep(10000L)

    val testSparkSession = SparkSession.builder().appName("Dataset pipeline - memory classes").master("local[*]").getOrCreate()
    import testSparkSession.implicits._

    val newLineJsonData = testSparkSession.read.json(BasePath)
    val groupedByIdDigitsCount = newLineJsonData.groupByKey(row => row.getAs[Int]("id").toString.length)
    groupedByIdDigitsCount.count().foreachPartition((countersById: Iterator[(Int, Long)]) => {
      countersById.foreach {
        case (id, count) => println(s"[APP] Got ${id} and ${count}")
      }
    })
  }


}
