package com.waitingforcode.memoryimpact

object OomDatasetParallelWithSplitSizeDefined {

  def main(args: Array[String]): Unit = {
    prepareTestEnvironment()
    val sparkSession = parallelProcessingSession
    import sparkSession.implicits._
    sparkSession.conf.set("spark.sql.files.maxPartitionBytes", (OneGbInBytes/100).toString)  // 10MB
    val textDataset = sparkSession.read.textFile(Test1GbFile) // or Test1Gb1MbFile Test1GbFile
    textDataset.map(txt => s"abc${txt}")
      .foreach(txt => {})
  }

}
