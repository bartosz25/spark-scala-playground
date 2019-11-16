package com.waitingforcode.sparksummit2019.customstatestore

import java.util.Date

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

/**
  * To execute this test, add this to `build.sbt` and execute `sbt compile` to generate
  * SQLite files required by the embedded DynamoDB. I don't include them natively
  */
object InMemoryDataTest {

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Test custom state store")
      .config("spark.sql.streaming.stateStore.providerClass", "com.waitingforcode.sparksummit2019.customstatestore.LocalDynamoDbStateStoreProvider")
      .config("spark.sql.shuffle.partitions", "1") // 1 for easier debugging
      .master("local[*]").getOrCreate()
    import sparkSession.implicits._
    val inputStream = new MemoryStream[(Long, String)](1, sparkSession.sqlContext)
    val mappedResult = inputStream.toDS().toDF("id", "name")
    inputStream.addData((1L, "test10"), (1L, "test11"), (2L, "test20"))

    //val sessionTimeout = TimeUnit.MINUTES.toMillis(5)
    val sessionTimeout = 5000L
    val query = mappedResult
      .groupByKey(row => row.getAs[Long]("id"))
      .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(Mapping.mapStreamingLogsToSessions(sessionTimeout))
      .filter(state => state.isDefined)
      .writeStream
      .outputMode("update")
      .foreach(
        new ForeachWriter[Option[String]] {
          override def open(partitionId: Long, epochId: Long): Boolean = {
            true
          }

          override def process(value: Option[String]): Unit = {
            println(s"processing ${value}")
          }

          override def close(errorOrNull: Throwable): Unit = {}
        }
      ).start()
    new Thread(new Runnable() {
      override def run(): Unit = {
        while (!query.isActive) {}
        var key = 1L
        var counter = 0
        while (true) {
          Thread.sleep(2500)
          inputStream.addData((key, s"test${new Date()}"))
          counter += 1
          if (counter%5 == 0) {
            key += 1
          }
        }
      }
    }).start()
    query.awaitTermination(30000)

  }

}

object Mapping {

  def mapStreamingLogsToSessions(timeoutDurationMs: Long)(key: Long, logs: Iterator[Row],
                                                          currentState: GroupState[Seq[String]]): Option[String] = {
    if (currentState.hasTimedOut) {
      println(s"Expiring state for ${key}")
      val expiredState = currentState.get
      currentState.remove()
      Some(expiredState.mkString(", "))
    } else {
      val newLogs = logs.map(log => log.getAs[String]("name"))
      val oldLogs = currentState.getOption.getOrElse(Seq.empty)
      currentState.update(oldLogs ++ newLogs)
      currentState.setTimeoutDuration(timeoutDurationMs)
      None
    }
  }

}