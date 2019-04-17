package com.waitingforcode.sql

import com.waitingforcode.util.store.InMemoryKeyedStore
import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.{FlatSpec, Matchers}


class MonotonicallyIncreasingIdTest extends FlatSpec with Matchers {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL monotonically increasing id").master("local[*]").getOrCreate()

  import sparkSession.implicits._

  "the same IDs" should "be generated for different data" in {
    println(s"${java.lang.Long.toBinaryString(1L)}")
    val datasetPeriod1 = (1 to 5).toDF("nr").repartition(5)
    datasetPeriod1.withColumn("primary_key", functions.monotonically_increasing_id())
      .foreach(row => {
        InMemoryKeyedStore.addValue(row.getAs[Long]("primary_key").toString, row.getAs[Int]("nr").toString)
      })
    InMemoryKeyedStore.allValues should have size 5
    InMemoryKeyedStore.allValues.foreach {
      case (key, data) => data should have size 1
    }

    val datasetPeriod2 = (6 to 10).toDF("nr").repartition(5)
    datasetPeriod2.withColumn("primary_key", functions.monotonically_increasing_id())
      .foreach(row => {
        InMemoryKeyedStore.addValue(row.getAs[Long]("primary_key").toString, row.getAs[Int]("nr").toString)
      })
    InMemoryKeyedStore.allValues should have size 5
    InMemoryKeyedStore.getValues("8589934594") should contain allOf("4", "9")
    InMemoryKeyedStore.getValues("34359738368") should contain allOf("3", "8")
    InMemoryKeyedStore.getValues("8589934593") should contain allOf("2", "7")
    InMemoryKeyedStore.getValues("0") should contain allOf("5", "10")
    InMemoryKeyedStore.getValues("8589934592") should  contain allOf("1", "6")
  }

  "the different IDs" should "be generated for different data thanks to the composite key from timestamp" in {
    val datasetPeriod1 = (1 to 5).toDF("nr").repartition(5)
    val generationTimePeriod1 = System.currentTimeMillis()
    sparkSession.sparkContext.broadcast(generationTimePeriod1)
    datasetPeriod1.withColumn("primary_key", functions.monotonically_increasing_id())
      .foreach(row => {
        val compositeKey = s"${generationTimePeriod1}_${row.getAs[Long]("primary_key")}"
        InMemoryKeyedStore.addValue(compositeKey, row.getAs[Int]("nr").toString)
      })
    InMemoryKeyedStore.allValues should have size 5
    InMemoryKeyedStore.allValues.foreach {
      case (key, data) => data should have size 1
    }

    val generationTimePeriod2 = System.currentTimeMillis()
    sparkSession.sparkContext.broadcast(generationTimePeriod2)
    val datasetPeriod2 = (6 to 10).toDF("nr").repartition(5)
    datasetPeriod2.withColumn("primary_key", functions.monotonically_increasing_id())
      .foreach(row => {
        val compositeKey = s"${generationTimePeriod2}_${row.getAs[Long]("primary_key")}"
        InMemoryKeyedStore.addValue(compositeKey, row.getAs[Int]("nr").toString)
      })
    InMemoryKeyedStore.allValues should have size 10
    InMemoryKeyedStore.allValues.foreach {
      case (key, data) => data should have size 1
    }
  }

  "the different IDs" should "be generated for different data thanks to the composite key from version" in {
    val datasetPeriod1 = (1 to 5).toDF("nr").repartition(5)
    val version1 = "version1"
    sparkSession.sparkContext.broadcast(version1)
    datasetPeriod1.withColumn("primary_key", functions.monotonically_increasing_id())
      .foreach(row => {
        val compositeKey = s"${version1}_${row.getAs[Long]("primary_key")}"
        InMemoryKeyedStore.addValue(compositeKey, row.getAs[Int]("nr").toString)
      })

    InMemoryKeyedStore.allValues should have size 5
    InMemoryKeyedStore.allValues.foreach {
      case (key, data) => data should have size 1
    }

    val version2 = "version2"
    sparkSession.sparkContext.broadcast(version2)
    val datasetPeriod2 = (6 to 10).toDF("nr").repartition(5)
    datasetPeriod2.withColumn("primary_key", functions.monotonically_increasing_id())
      .foreach(row => {
        val compositeKey = s"${version2}_${row.getAs[Long]("primary_key")}"
        InMemoryKeyedStore.addValue(compositeKey, row.getAs[Int]("nr").toString)
      })

    InMemoryKeyedStore.allValues should have size 10
    InMemoryKeyedStore.allValues.foreach {
      case (key, data) => data should have size 1
    }
  }

}
