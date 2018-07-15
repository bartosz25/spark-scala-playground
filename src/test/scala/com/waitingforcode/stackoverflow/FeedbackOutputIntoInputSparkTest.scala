package com.waitingforcode.stackoverflow

import java.io.File
import java.sql.PreparedStatement
import java.time.LocalDateTime
import java.util.concurrent.{ConcurrentHashMap, ThreadLocalRandom}

import com.waitingforcode.util.InMemoryDatabase
import com.waitingforcode.util.sql.data.DataOperation
import org.apache.commons.io.FileUtils
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable

/**
  * Proposal for:
  * https://stackoverflow.com/questions/50375497/how-to-run-a-continuous-batch-process-in-spark/50397618?noredirect=1#comment87850687_50397618
  *
  * Rationale:
  * Executing a batch job every 10 minutes largely looks like executing micro batches.
  * In Spark Structured Streaming every micro batch waits its predecessor to terminate (with default trigger used).
  * Since the proposal above should work even if the processing takes 10 minutes or more. The sequential execution
  * of micro batches is proven in the final assertions comparing minInsertTime and maxInsertTime that should be
  * greater than 5 seconds (sleep inside ForeachWriter method closing given stream)
  *
  * Similar question to Flink with solution: https://stackoverflow.com/questions/35736756/spark-streaming-how-to-feedback-output-into-input
  */
class FeedbackOutputIntoInputSparkTest extends FlatSpec with BeforeAndAfterAll with BeforeAndAfter with Matchers {

  override def beforeAll(): Unit = {
    new File(Configuration.ComputationTriggersDirectory).mkdir()
    new File(Configuration.EnrichmentDataDirectory).mkdir()
    FileUtils.cleanDirectory(new  File(Configuration.ComputationTriggersDirectory))
    FileUtils.cleanDirectory(new File(Configuration.EnrichmentDataDirectory))
    InMemoryDatabase.cleanDatabase()
    InMemoryDatabase.createTable("CREATE TABLE information(id bigint auto_increment primary key, version text, " +
      "content text, partitionNr int, insertTime long, metadataId bigint, metadataValue text)")
    val currentVersion = Long.MinValue
    val informationTuples = (1 to 10).map(id => (s"${Configuration.RunId}${currentVersion}", s"/${currentVersion}", id))
    val informationRowsToInsert = mutable.ListBuffer[InformationDataOperation]()
    for (information <- informationTuples) {
      informationRowsToInsert.append(InformationDataOperation(information._1, information._2, information._3, "init_metadata"))
    }
    InMemoryDatabase.populateTable("INSERT INTO information (version, content, partitionNr, insertTime, metadataId, metadataValue) " +
      "VALUES (?, ?, ?, ?, ?, ?)", informationRowsToInsert)
    FileUtils.writeStringToFile(new File(s"${Configuration.ComputationTriggersDirectory}${System.currentTimeMillis()}"),
      s"${Configuration.RunId}${currentVersion}")
  }

  override def afterAll() {
    InMemoryDatabase.cleanDatabase()
  }

  val sparkSession: SparkSession = SparkSession.builder().appName("Spark Structured Streaming stateful aggregation")
    .master("local[*]").getOrCreate()
  import sparkSession.implicits._

  "streaming job" should "be executed once the marker file is uploaded" in {
    val currentDate = LocalDateTime.now().toString
    val metadataContent = (1 to 10).map(nr => "{\"metadata_id_file\": "+nr+", \"metadata\": \""+currentDate+"\"}")
      .mkString("\n")
    FileUtils.writeStringToFile(new File(s"${Configuration.EnrichmentDataDirectory}/metadata.json"), metadataContent)

    val triggerFiles = sparkSession.readStream.text(s"${Configuration.ComputationTriggersDirectory}*")
    val enrichmentFiles = sparkSession.read.json(s"${Configuration.EnrichmentDataDirectory}*")
    val dataTable = getH2DataFrame("information", sparkSession)

    val dataWithMetadata = dataTable.join(triggerFiles, $"value" === $"version")
      .repartition(10)
      .join(enrichmentFiles, $"metadataId" === $"metadata_id_file")
    val streamingQuery = dataWithMetadata.writeStream.foreach(new ForeachWriter[Row] {

      private var version: Long = Long.MinValue

      override def open(partitionId: Long, version: Long): Boolean = {
        //println(s"Opening ${partitionId} for version ${version}")
        this.version = version
        true
      }

      override def process(row: Row): Unit = {
        val newRow = InformationDataOperation(s"${Configuration.RunId}${version}",
          s"${row.getAs[String]("CONTENT")}/${Configuration.RunId}${version}",
          row.getAs[Long]("METADATAID"), row.getAs[String]("metadata"))
        InMemoryDatabase.populateTable("" +
          "INSERT INTO information (version, content, partitionNr, insertTime, metadataId, metadataValue) " +
          "VALUES (?, ?, ?, ?, ?, ?)",
          Seq(newRow))
        println(s"Processing for version ${version}")
      }

      override def close(errorOrNull: Throwable): Unit = {
        FileUtils.writeStringToFile(new File(s"${Configuration.ComputationTriggersDirectory}${version}"),
          s"${Configuration.RunId}${version}")
        // Just gives some time to analyze the output
        //Thread.sleep(5000)
      }

    }).start()

    streamingQuery.awaitTermination(1000*60)

    val addedRows = InMemoryDatabase.getRows("SELECT * FROM information ORDER BY version ASC", (resultSet) => {
      (resultSet.getString("version"), resultSet.getString("content"), resultSet.getInt("partitionNr"))
    })
    //println(s"Got added rows ${addedRows.mkString("\n")}")

    val insertedRows = InMemoryDatabase.getRows("SELECT version, MIN(insertTime) AS minInsertTime,  " +
      "MAX(insertTime) AS maxInsertTime FROM information GROUP BY version ORDER BY version ASC", (resultSet) => {
      (resultSet.getString("version"), resultSet.getLong("minInsertTime"), resultSet.getLong("maxInsertTime"))
    })
    val insertedRowsIterator = insertedRows.iterator
    var rowToCompare = insertedRowsIterator.next()
    while (insertedRowsIterator.hasNext) {
      val nextRow = insertedRowsIterator.next()
      val diffMaxMin = nextRow._2 - rowToCompare._3
      //diffMaxMin should be > 5000L
      println(s"diffMaxMin=${diffMaxMin}")
      rowToCompare = nextRow
    }
    val metadataRows = InMemoryDatabase.getRows("SELECT DISTINCT(metadataValue) AS distinctMetadataValue " +
      "FROM information", (resultSet) => {
      resultSet.getString("distinctMetadataValue")
    })
    println(s"metadatRow=${metadataRows}")
  }



  private def getH2DataFrame(tableName: String, sparkSession: SparkSession): DataFrame = {
    val OptionsMap: Map[String, String] =
      Map("url" -> InMemoryDatabase.DbConnection, "user" -> InMemoryDatabase.DbUser, "password" -> InMemoryDatabase.DbPassword,
        "driver" ->  InMemoryDatabase.DbDriver,
        "partitionColumn" -> "partitionNr", "numPartitions" -> "3", "lowerBound" -> "0", "upperBound" -> "10")
    val jdbcOptions = OptionsMap ++ Map("dbtable" -> tableName)
    sparkSession.read.format("jdbc")
      .options(jdbcOptions)
      .load()

  }
}

object Configuration {
  val ComputationTriggersDirectory = "/tmp/computation_triggers/"
  val EnrichmentDataDirectory = "/tmp/enrichment_data"
  val RunId = "run#1"
}

object DataStorage {
  val DataPerMicroBatch = new ConcurrentHashMap[Long, mutable.ListBuffer[String]]()
}

case class InformationDataOperation(version: String, content: String, metadataId: Long, metadataValue: String) extends DataOperation {
  override def populatePreparedStatement(preparedStatement: PreparedStatement): Unit = {
    preparedStatement.setString(1, version)
    preparedStatement.setString(2, content)
    preparedStatement.setInt(3, ThreadLocalRandom.current().nextInt(11))
    preparedStatement.setLong(4, System.currentTimeMillis())
    preparedStatement.setLong(5, metadataId)
    preparedStatement.setString(6, metadataValue)
  }
}