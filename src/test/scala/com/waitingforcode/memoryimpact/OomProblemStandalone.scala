package com.waitingforcode.memoryimpact

object OomProblemStandalone {

  def main(args: Array[String]): Unit = {
    prepareTestEnvironment()
    val sparkSession = standaloneSession
    val textRdd = sparkSession.sparkContext.textFile(Test3GbFile)
    processTextRdd(textRdd)
  }

}
