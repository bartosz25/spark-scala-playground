package com.waitingforcode.memoryimpact


object OomParallelWithSplitSizeDefined {

  def main(args: Array[String]): Unit = {
    prepareTestEnvironment()
    val sparkSession = parallelProcessingSession
    val textRdd = sparkSession.sparkContext.textFile(Test1GbFile)
    processTextRdd(textRdd)
  }

}
