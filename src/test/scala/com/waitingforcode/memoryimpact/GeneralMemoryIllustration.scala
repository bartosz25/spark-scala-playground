package com.waitingforcode.memoryimpact

import org.apache.spark.storage.StorageLevel


object GeneralMemoryIllustration {

  def main(args: Array[String]): Unit = {
    prepareTestEnvironment()
    val textRdd = sequentialProcessingSession.sparkContext.textFile(Test500MbFile)
    textRdd.persist(StorageLevel.MEMORY_ONLY)
    processTextRdd(textRdd)
  }

}
