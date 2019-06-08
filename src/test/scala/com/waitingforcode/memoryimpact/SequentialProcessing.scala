package com.waitingforcode.memoryimpact

object SequentialProcessing {

  def main(args: Array[String]): Unit = {
    prepareTestEnvironment()
    val sparkSession = sequentialProcessingSession
    val textRdd = sparkSession.sparkContext.textFile(Test1GbFile)
    processTextRdd(textRdd)
  }

}
