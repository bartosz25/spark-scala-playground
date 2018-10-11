package com.waitingforcode.memoryimpact

object OomProblemStandalone {

  def main(args: Array[String]): Unit = {
    prepareTestEnvironment()
    val sparkSession = standaloneSession
    sparkSession.sparkContext
      .hadoopConfiguration.set("mapreduce.input.fileinputformat.split.minsize", (OneGbInBytes/100).toString) // 10MB
    sparkSession.sparkContext
      .hadoopConfiguration.set("mapreduce.input.fileinputformat.split.maxsize", (OneGbInBytes/100).toString) // 10MB
    val textRdd = sparkSession.sparkContext.textFile(Test3GbFile)
    processTextRdd(textRdd)
  }

}
