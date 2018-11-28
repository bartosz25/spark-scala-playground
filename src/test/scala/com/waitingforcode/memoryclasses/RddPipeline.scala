package com.waitingforcode.memoryclasses

import com.waitingforcode.{NumberContent, NumberContentLabels}
import com.waitingforcode.util.javaassist.SparkMemoryClassesDecorators
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSON


object RddPipeline {

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

    val testSparkSession = SparkSession.builder().appName("RDD pipeline - memory classes").master("local[*]").getOrCreate()

    val jsonLineRdd = testSparkSession.sparkContext.textFile(BasePath)
    val mappedJsonToNumberContents = jsonLineRdd.map(json => {
      val jsonContent = JSON.parseFull(json)
      val jsonMap = jsonContent.get.asInstanceOf[Map[String, Any]]
      val labels = jsonMap("labels").asInstanceOf[Map[String, String]]
      NumberContent(jsonMap("id").asInstanceOf[Double].toInt,
        NumberContentLabels(labels("short"), labels("long")), jsonMap("creation_time_ms").asInstanceOf[Double].toLong)
    })

    val groupedByIdDigitsCount = mappedJsonToNumberContents.groupBy(numberContent => numberContent.id.toString.length)
    groupedByIdDigitsCount.sortBy {
      case (digitsCount, _) => digitsCount
    }.foreachPartition(partitionDataIterator => {
      partitionDataIterator.foreach {
        case (digitsCount, items) => {
          println(s"[APP] Got digitsCount=${digitsCount}")
        }
      }
    })

  }


}
