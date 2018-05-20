package com.waitingforcode.structuredstreaming

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.io.Source
import scala.reflect.io.{File, Path}

class FaultToleranceTest extends FlatSpec with Matchers with BeforeAndAfterAll with Serializable {

  override def beforeAll(): Unit = {
    Path(TestConfiguration.TestDirInput).createDirectory()
    Path(TestConfiguration.TestDirOutput).createDirectory()
    for (i <- 1 to 10) {
      val file = s"file${i}"
      val content =
        s"""
          |{"id": 1, "name": "content1=${i}"}
          |{"id": 2, "name": "content2=${i}"}
        """.stripMargin
      File(s"${TestConfiguration.TestDirInput}/${file}").writeAll(content)
    }
  }

  override def afterAll(): Unit = {
    Path(TestConfiguration.TestDirInput).deleteRecursively()
    Path(TestConfiguration.TestDirOutput).deleteRecursively()
  }


  "after one failure" should "all rows should be processed and output in idempotent manner" in {
    for (i <- 0 until 2) {
      val sparkSession: SparkSession = SparkSession.builder()
        .appName("Spark Structured Streaming fault tolerance example")
        .master("local[2]").getOrCreate()
      import sparkSession.implicits._
      try {
        val schema = StructType(StructField("id", LongType, nullable = false) ::
          StructField("name", StringType, nullable = false) :: Nil)

        val idNameInputStream = sparkSession.readStream.format("json").schema(schema).option("maxFilesPerTrigger", 1)
          .load(TestConfiguration.TestDirInput)
          .toDF("id", "name")
          .map(row => {
            val fieldName = row.getAs[String]("name")
            if (fieldName == "content1=7" && !GlobalFailureFlag.alreadyFailed.get() && GlobalFailureFlag.mustFail.get()) {
              GlobalFailureFlag.alreadyFailed.set(true)
              GlobalFailureFlag.mustFail.set(false)
              throw new RuntimeException("Something went wrong")
            }
            fieldName
          })

        val query = idNameInputStream.writeStream.outputMode("update").foreach(new ForeachWriter[String] {
          override def process(value: String): Unit = {
            val fileName = value.replace("=", "_")
            File(s"${TestConfiguration.TestDirOutput}/${fileName}").writeAll(value)
          }

          override def close(errorOrNull: Throwable): Unit = {}

          override def open(partitionId: Long, version: Long): Boolean = true
        }).start()
        query.awaitTermination(15000)
      } catch {
        case re: StreamingQueryException => {
          println("As expected, RuntimeException was thrown")
        }
      }
    }

    val outputFiles = Path(TestConfiguration.TestDirOutput).toDirectory.files.toSeq
    outputFiles should have size 20
    val filesContent = outputFiles.map(file => Source.fromFile(file.path).getLines.mkString(""))
    val expected = (1 to 2).flatMap(id => {
      (1 to 10).map(nr => s"content${id}=${nr}")
    })
    filesContent should contain allElementsOf expected
  }

}

// Must define the dirs in separate object because of not serializable task exception:
/*
	- object not serializable (class: org.scalatest.Assertions$AssertionsHelper, value: org.scalatest.Assertions$AssertionsHelper@7c7ed72a)
	- field (class: org.scalatest.FlatSpec, name: assertionsHelper, type: class org.scalatest.Assertions$AssertionsHelper)
	- object (class com.waitingforcode.structuredstreaming.FaultToleranceTest, FaultToleranceTest)
	- field (class: com.waitingforcode.structuredstreaming.FaultToleranceTest$$anonfun$1, name: $outer, type: class com.waitingforcode.structuredstreaming.FaultToleranceTest)
	- object (class com.waitingforcode.structuredstreaming.FaultToleranceTest$$anonfun$1, <function0>)
	- field (class: com.waitingforcode.structuredstreaming.FaultToleranceTest$$anonfun$1$$anonfun$apply$1, name: $outer, type: class com.waitingforcode.structuredstreaming.FaultToleranceTest$$anonfun$1)
	- object (class com.waitingforcode.structuredstreaming.FaultToleranceTest$$anonfun$1$$anonfun$apply$1, <function1>)
	- field (class: com.waitingforcode.structuredstreaming.FaultToleranceTest$$anonfun$1$$anonfun$apply$1$$anon$1, name: $outer, type: class com.waitingforcode.structuredstreaming.FaultToleranceTest$$anonfun$1$$anonfun$apply$1)
	- object (class com.waitingforcode.structuredstreaming.FaultToleranceTest$$anonfun$1$$anonfun$apply$1$$anon$1, com.waitingforcode.structuredstreaming.FaultToleranceTest$$anonfun$1$$anonfun$apply$1$$anon$1@5e5a8718)
 */
object TestConfiguration {
  val TestDirInput = "/tmp/spark-fault-tolerance"
  val TestDirOutput = s"${TestDirInput}-output"
}

object GlobalFailureFlag {
  var mustFail = new AtomicBoolean(true)
  var alreadyFailed = new AtomicBoolean(false)
}
