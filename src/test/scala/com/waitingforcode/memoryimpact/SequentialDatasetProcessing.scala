package com.waitingforcode.memoryimpact

object SequentialDatasetProcessing {

  def main(args: Array[String]): Unit = {
    prepareTestEnvironment()
    val sparkSession = sequentialProcessingSession
    import sparkSession.implicits._
    sparkSession.conf.set("spark.sql.files.maxPartitionBytes", (OneGbInBytes/100).toString)  // 10MB

    val textDataset = sparkSession.read.textFile(Test1Gb1MbFile)

    textDataset.map(txt => txt)
      .foreach(txt => {})
  }
}
