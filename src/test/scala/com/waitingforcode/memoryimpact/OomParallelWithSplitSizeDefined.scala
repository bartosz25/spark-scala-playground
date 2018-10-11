package com.waitingforcode.memoryimpact


object OomParallelWithSplitSizeDefined {

  def main(args: Array[String]): Unit = {
    prepareTestEnvironment()
    val sparkSession = parallelProcessingSession
    sparkSession.sparkContext
      .hadoopConfiguration.set("mapreduce.input.fileinputformat.split.minsize", (OneGbInBytes/100).toString) // 10MB
    sparkSession.sparkContext
      .hadoopConfiguration.set("mapreduce.input.fileinputformat.split.maxsize", (OneGbInBytes/100).toString) // 10MB
    val textRdd = sparkSession.sparkContext.textFile(Test1GbFile)
    processTextRdd(textRdd)
  }

}
