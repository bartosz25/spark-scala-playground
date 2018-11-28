package com.waitingforcode.sql

import java.io.File
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.waitingforcode.util.InMemoryLogAppender
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class SparkSessionTest extends FlatSpec with Matchers {

  "Spark" should "create session and override configuration properties" in {
    val testedSession = SparkSession.builder().config("spark.executor.memory", "2g").appName("Tested app").master("local")
      .getOrCreate()
    val configMapAfterCreation = testedSession.conf.getAll

    testedSession.conf.set("spark.executor.memory", "1g")
    val configMapAfterOverriding = testedSession.conf.getAll

    configMapAfterCreation("spark.executor.memory") shouldEqual "2g"
    configMapAfterOverriding("spark.executor.memory") shouldEqual "1g"
    testedSession.close()
  }

  "Spark" should "create 1 instance of SparkSession with builder"in {
    val sparkSession1 = SparkSession.builder().appName("SparkSession#1").master("local").getOrCreate()
    val sparkSession2 = SparkSession.builder().appName("SparkSession#2").master("local").getOrCreate()

    sparkSession1 shouldEqual sparkSession2
  }

  "Spark" should "create 2 SparkSessions" in {
    val sparkSession1 = SparkSession.builder().appName("SparkSession#1").master("local").getOrCreate()
    val sparkSession2 = sparkSession1.newSession()

    sparkSession1.sparkContext shouldEqual sparkSession2.sparkContext
    sparkSession1.stop()
    // Since both SparkContexts are equal, stopping the one for sparkSession1 will make the context of
    // sparkSession2 stopped too
    sparkSession2.sparkContext.isStopped shouldBe true
    // and that despite the different sessions
    sparkSession1 shouldNot equal(sparkSession2)
    sparkSession1.close()
  }

  "Spark" should "launch 2 different apps for reading JSON files" in {
    val commonDataFile = new File("/tmp/spark_sessions/common_data.jsonl")
    val commonData =
      """
        | {"id": 1, "name": "A"}
        | {"id": 2, "name": "B"}
        | {"id": 3, "name": "C"}
        | {"id": 4, "name": "D"}
        | {"id": 5, "name": "E"}
        | {"id": 6, "name": "F"}
        | {"id": 7, "name": "G"}
      """.stripMargin
    FileUtils.writeStringToFile(commonDataFile, commonData)
    val dataset1File = new File("/tmp/spark_sessions/dataset_1.jsonl")
    val dataset1Data =
      """
        | {"value": 100, "join_key": 1}
        | {"value": 300, "join_key": 3}
        | {"value": 500, "join_key": 5}
        | {"value": 700, "join_key": 7}
      """.stripMargin
    FileUtils.writeStringToFile(dataset1File, dataset1Data)
    val dataset2File = new File("/tmp/spark_sessions/dataset_2.jsonl")
    val dataset2Data =
      """
        | {"value": 200, "join_key": 2}
        | {"value": 400, "join_key": 4}
        | {"value": 600, "join_key": 6}
      """.stripMargin
    FileUtils.writeStringToFile(dataset2File, dataset2Data)
    // Executed against standalone cluster to better see that there is only 1 Spark application created
    val sparkSession = SparkSession.builder().appName(s"SparkSession for 2 different sources").master("local")
      .config("spark.executor.extraClassPath", sys.props("java.class.path"))
      .getOrCreate()
    val commonDataset = sparkSession.read.json(commonDataFile.getAbsolutePath)
    commonDataset.cache()
    import org.apache.spark.sql.functions._
    val oddNumbersDataset = sparkSession.read.json(dataset1File.getAbsolutePath)
      .join(commonDataset, col("id") === col("join_key"), "left")
    val oddNumbers = oddNumbersDataset.collect()


    // Without stop the SparkSession is represented under the same name in the UI and the master remains the same
    // sparkSession.stop()
    // But if you stop the session you won't be able to join the data from the second session with a dataset from the first session
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val sparkSession2 = SparkSession.builder().appName(s"Another Spark session").master("local")
      .config("spark.executor.extraClassPath", sys.props("java.class.path"))
      .getOrCreate()

    SparkSession.setDefaultSession(sparkSession2)
    val pairNumbersDataset = sparkSession2.read.json(dataset2File.getAbsolutePath)
      .join(commonDataset, col("id") === col("join_key"), "left")

    val pairNumbers = pairNumbersDataset.collect()

    sparkSession shouldNot equal(sparkSession2)
    def stringifyRow(row: Row): String = {
      s"${row.getAs[Int]("id")}-${row.getAs[String]("name")}-${row.getAs[Int]("value")}"
    }
    val oddNumbersMapped = oddNumbers.map(stringifyRow(_))
    oddNumbersMapped should have size 4
    oddNumbersMapped should contain allOf("1-A-100", "3-C-300", "5-E-500", "7-G-700")
    val pairNumbersMapped = pairNumbers.map(stringifyRow(_))
    pairNumbersMapped should have size 3
    pairNumbersMapped should contain allOf("2-B-200", "4-D-400", "6-F-600")

    sparkSession2.close()
  }

  "Spark" should "launch 3 applications in 3 different threads" in {
    val logAppender = InMemoryLogAppender.createLogAppender(Seq("SparkSession#0",
      "SparkSession#1", "SparkSession#2"))
    val latch = new CountDownLatch(3)
    (0 until 3).map(nr => new Thread(new Runnable() {
      override def run(): Unit = {
        // You can submit this application to a standalone cluster to see in the UI that always only 1 app name
        // is picked up and despite of that, all 3 applications are executed inside
        val config = new SparkConf().setMaster("local").setAppName(s"SparkSession#${nr}")
          .set("spark.executor.extraClassPath", sys.props("java.class.path"))
        val sparkSession = SparkSession.builder().config(config)
          .getOrCreate()
        import sparkSession.implicits._
        val dataset = (0 to 5).map(nr => (nr, s"${nr}")).toDF("id", "label")
        val rowsIds = dataset.collect().map(row => row.getAs[Int]("id")).toSeq
        // Give some time to make the observations
        Thread.sleep(3000)
        println(s"adding ${rowsIds}")
        AccumulatedRows.addData(rowsIds)
        latch.countDown()
      }
    }).start())
    latch.await(3, TimeUnit.MINUTES)
    // Add a minute to prevent against race conditions
    Thread.sleep(5000L)

    AccumulatedRows.data.size shouldEqual 3
    AccumulatedRows.data.foreach(rows => rows should contain allOf(0, 1, 2, 3, 4, 5))
    println(logAppender.getMessagesText())
    logAppender.getMessagesText() should have size 1
    // Since it's difficult to deduce which application was submitted, we check only the beginning of the log message
    val firstMessage = logAppender.getMessagesText()(0)
    firstMessage should startWith("Submitted application: SparkSession#")
    val Array(_, submittedSessionId) = firstMessage.split("#")
    val allSessions = Seq("0", "1", "2")
    val missingSessionsFromLog = allSessions.diff(Seq(submittedSessionId))
    missingSessionsFromLog should have size 2
  }


}

object AccumulatedRows {
  val data = new scala.collection.mutable.ListBuffer[Seq[Int]]()
  def addData(nr: Seq[Int]): Unit = {
    data.synchronized {
      data.appendAll(Seq(nr))
    }
  }

}
