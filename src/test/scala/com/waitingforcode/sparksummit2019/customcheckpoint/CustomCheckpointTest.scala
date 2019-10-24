package com.waitingforcode.sparksummit2019.customcheckpoint

import com.waitingforcode.util.InMemoryStoreWriter
import org.apache.spark.sql.streaming.{GroupState, OutputMode}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class CustomCheckpointTest extends FlatSpec with Matchers {

  val sparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming custom checkpoint")
    .config("spark.sql.shuffle.partitions", 5)
    .config("spark.sql.streaming.checkpointFileManagerClass", "com.waitingforcode.sparksummit2019.customcheckpoint.JavaNioCheckpointFileManager")
    .master("local[1]").getOrCreate()
  import sparkSession.implicits._

  private val MappingFunction: (Long, Iterator[Row], GroupState[Seq[String]]) => Seq[String] = (_, values, state) => {
    val stateNames = state.getOption.getOrElse(Seq.empty)
    val stateNewNames = stateNames ++ values.map(row => row.getAs[String]("name"))
    state.update(stateNewNames)
    stateNewNames
  }

  "the state" should "be initialized for the same data source" in {
    val testKey = "state-init-same-source-mode"
    val schema = StructType(
      Seq(StructField("id", DataTypes.LongType, false), StructField("name", DataTypes.StringType, false))
    )

    val sourceDir = "/tmp/batch-state-init"
    val stateDataset = Seq((1L, "old_page1"), (2L, "old_page2")).toDF("id", "name")
    stateDataset.write.mode(SaveMode.Overwrite).json(sourceDir)

    val stateQuery = sparkSession.readStream
      .schema(schema)
      .json(sourceDir).groupByKey(row => row.getAs[Long]("id"))
      .mapGroupsWithState(MappingFunction)
      .writeStream
      .option("checkpointLocation", "/tmp/batch-checkpoint")
      .outputMode(OutputMode.Update())
      .foreach(new InMemoryStoreWriter[Seq[String]](testKey, (stateSeq) => stateSeq.mkString(",")))
      .start()
    stateQuery.awaitTermination(45000)
    stateQuery.stop()

    ActionsTracker.ExistencyChecks should not be empty
    ActionsTracker.MakeDirsCalls should not be empty
    ActionsTracker.CreateAtomicCalls should not be empty
  }
}
