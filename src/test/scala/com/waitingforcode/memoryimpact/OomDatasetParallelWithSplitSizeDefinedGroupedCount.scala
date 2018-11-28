package com.waitingforcode.memoryimpact

object OomDatasetParallelWithSplitSizeDefinedGroupedCount {

  def main(args: Array[String]): Unit = {
    prepareTestEnvironment()
    val sparkSession = standaloneSession
    import sparkSession.implicits._
    sparkSession.conf.set("spark.sql.files.maxPartitionBytes", (OneGbInBytes/100).toString)  // 10MB
    val textDataset = sparkSession.read.textFile(Test1GbFile) // or Test1Gb1MbFile
    textDataset.map(txt => s"abc${txt}")
      .groupBy($"value")
      .count()
  }

}
