package com.waitingforcode.sql

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class DatasetReductionTest extends FlatSpec with Matchers {

  private val sparkSession = SparkSession.builder().appName("Spark SQL dataset reduction").master("local[*]").getOrCreate()
  import sparkSession.implicits._

  "dataset projection" should "reduce the size of stored data" in {
    val dataset = Seq(
      (1, "a", "A"),
      (2, "b", "B"),
      (3, "c", "C"),
      (4, "d", "D")
    ).toDF("nr", "lower_case_letter", "upper_case_letter")

    val reducedDataset = dataset.select("nr", "lower_case_letter")

    reducedDataset.collect().map(row => s"${row.getAs[Int]("nr")}-${row.getAs[String]("lower_case_letter")}") should
      contain allOf("1-a", "2-b", "3-c", "4-d")
  }

  "vertical partitioning" should "reduce the size of dataset in a K/V store" in {
    val dataset = Seq(
      (1, "a", "A"),
      (2, "b", "B"),
      (3, "c", "C"),
      (4, "d", "D")
    ).toDF("nr", "lower_case_letter", "upper_case_letter")

    dataset.foreachPartition(rows => {
      // Always prefer batch operations - less network communication, data store queried only once for n keys
      rows.grouped(100)
        .foreach(rowsBatch => {
          val mappedLowerCases = rowsBatch.map(row => (row.getAs[Int]("nr"), row.getAs[String]("lower_case_letter")))
          val mappedUpperCases = rowsBatch.map(row => (row.getAs[Int]("nr"), row.getAs[String]("upper_case_letter")))
          KeyValueStore.Letters.addAllLowerCase(mappedLowerCases)
          KeyValueStore.Letters.addAllUpperCase(mappedUpperCases)
        })
    })

    KeyValueStore.Letters.allLowerCase should contain allOf (1 -> "a", 2 -> "b", 3 -> "c", 4 -> "d")
    KeyValueStore.Letters.allUpperCase should contain allOf (1 -> "A", 2 -> "B", 3 -> "C", 4 -> "D")
  }

  "a serialization framework" should "reduce the size of dataset" in {
    val dataset = (0 to 1000).map(nr => (nr, s"number${nr}")).toDF("nr", "label")


    val avroOutput = "/tmp/dataset-avro"
    val jsonOutput = "/tmp/dataset-json"
    dataset.write.format("avro").mode(SaveMode.Overwrite).save(avroOutput)
    dataset.write.format("json").mode(SaveMode.Overwrite).save(jsonOutput)

    val avroFilesSize = FileUtils.sizeOfDirectory(new File(avroOutput))
    val jsonFilesSize = FileUtils.sizeOfDirectory(new File(jsonOutput))
    println(s"avro=${avroFilesSize} vs json=${jsonFilesSize}")
    jsonFilesSize should be > avroFilesSize
  }

  "compressed files" should "reduce the size of dataset" in {
    val dataset = (0 to 1000).map(nr => (nr, s"number${nr}")).toDF("nr", "label")

    val rawOutput = "/tmp/dataset-json-raw"
    val compressedOutput = "/tmp/dataset-json-compressed"
    dataset.write.format("json").mode(SaveMode.Overwrite).save(rawOutput)
    dataset.write.format("json").option("compression", "gzip").mode(SaveMode.Overwrite).save(compressedOutput)

    val rawOutputFileSize = FileUtils.sizeOfDirectory(new File(rawOutput))
    val compressedOutputFileSize = FileUtils.sizeOfDirectory(new File(compressedOutput))
    println(s"raw=${rawOutputFileSize} vs compressed=${compressedOutputFileSize}")
    rawOutputFileSize should be > compressedOutputFileSize
  }

}

object KeyValueStore {

  object Letters {
    private val LowerCase = new mutable.HashMap[Int, String]()
    private val UpperCase = new mutable.HashMap[Int, String]()

    def addAllLowerCase(rows: Seq[(Int, String)]) = {
      rows.foreach {
        case (key, value) => LowerCase.put(key, value)
      }
    }
    def addAllUpperCase(rows: Seq[(Int, String)]) = {
      rows.foreach {
        case (key, value) => UpperCase.put(key, value)
      }
    }

    def allLowerCase = LowerCase
    def allUpperCase = UpperCase
  }

}

