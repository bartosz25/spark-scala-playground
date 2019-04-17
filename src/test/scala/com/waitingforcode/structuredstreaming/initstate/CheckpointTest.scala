package com.waitingforcode.structuredstreaming.initstate

import java.io.File

import com.waitingforcode.util.InMemoryStoreWriter
import com.waitingforcode.util.store.InMemoryKeyedStore
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupState, OutputMode}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}

class CheckpointTest  extends FlatSpec with Matchers {

  val sparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming initial state")
    .config("spark.sql.shuffle.partitions", 5)
    .master("local[2]").getOrCreate()
  import sparkSession.implicits._

  private val MappingFunction: (Long, Iterator[Row], GroupState[Seq[String]]) => Seq[String] = (_, values, state) => {
    val stateNames = state.getOption.getOrElse(Seq.empty)
    val stateNewNames = stateNames ++ values.map(row => row.getAs[String]("name"))
    state.update(stateNewNames)
    stateNewNames
  }

  "the state" should "be initialized for the same data source" in {
    val testKey = "state-init-same-source-mode"
    val checkpointDir = s"/tmp/batch-checkpoint${System.currentTimeMillis()}"
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
      .option("checkpointLocation", checkpointDir)
      .outputMode(OutputMode.Update())
      .foreach(new InMemoryStoreWriter[Seq[String]](testKey, (stateSeq) => stateSeq.mkString(",")))
      .start()
    stateQuery.awaitTermination(45000)
    stateQuery.stop()

    val newInputData = Seq((1L, "page1"), (2L, "page2")).toDF("id", "name")
    newInputData.write.mode(SaveMode.Overwrite).json(sourceDir)
    val fileBasedQuery = sparkSession.readStream
      .schema(schema)
      .json(sourceDir).groupByKey(row => row.getAs[Long]("id"))
      .mapGroupsWithState(MappingFunction)
      .writeStream
      .option("checkpointLocation", checkpointDir)
      .outputMode(OutputMode.Update())
      .foreach(new InMemoryStoreWriter[Seq[String]](testKey, (stateSeq) => stateSeq.mkString(", ")))
      .start()
    fileBasedQuery.awaitTermination(45000)
    fileBasedQuery.stop()

    InMemoryKeyedStore.getValues(testKey) should have size 4
    InMemoryKeyedStore.getValues(testKey) should contain allOf("old_page2", "old_page1",
      "old_page2, page2", "old_page1, page1" )
  }

  "the state" should "not be initialized for different data sources" in {
    val testKey = "state-init-different-source-mode"
    val checkpointDir = s"/tmp/batch-checkpoint${System.currentTimeMillis()}"
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
      .option("checkpointLocation", checkpointDir)
      .outputMode(OutputMode.Update())
      .foreach(new InMemoryStoreWriter[Seq[String]](testKey, (stateSeq) => stateSeq.mkString(",")))
      .start()
    stateQuery.awaitTermination(45000)
    stateQuery.stop()

    // Cleans the checkpoint location and keeps only the state files
    cleanCheckpointLocation(checkpointDir)

    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val inputDataset = inputStream.toDS().toDF("id", "name")
    inputStream.addData((1L, "page1"), (2L, "page2"))
    val mappedValues = inputDataset
      .groupByKey(row => row.getAs[Long]("id"))
      .mapGroupsWithState(MappingFunction)
    val query = mappedValues.writeStream.outputMode("update")
      .option("checkpointLocation", checkpointDir)
      .foreach(new InMemoryStoreWriter[Seq[String]](testKey, (stateSeq) => stateSeq.mkString(","))).start()

    query.awaitTermination(60000)

    InMemoryKeyedStore.getValues(testKey) should have size 4
    InMemoryKeyedStore.getValues(testKey) should contain allOf("old_page2", "old_page1", "page2", "page1")
  }

  private def cleanCheckpointLocation(checkpointDir: String): Unit = {
    FileUtils.deleteDirectory(new File(s"${checkpointDir}/commits"))
    FileUtils.deleteDirectory(new File(s"${checkpointDir}/offsets"))
    FileUtils.deleteDirectory(new File(s"${checkpointDir}/sources"))
    new File(s"${checkpointDir}/metadata").delete()
  }

}
