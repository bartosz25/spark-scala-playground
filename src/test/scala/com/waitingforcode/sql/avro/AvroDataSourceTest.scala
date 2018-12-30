package com.waitingforcode.sql.avro

import java.io.{File, FileNotFoundException}

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.spark.SparkException
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class AvroDataSourceTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val OrderSchema =
    """
      |{
      |"type": "record",
      |"name": "order",
      |"fields": [
      | {"name": "id", "type": "int"},
      | {"name": "amount", "type": "double"}
      |]}
    """.stripMargin

  private val FileWithExtName = "./avro_data.avro"
  private val FileWithExt = new File(FileWithExtName)

  private val FileWithoutExtName = "./avro_data"
  private val FileWithoutExt = new File(FileWithoutExtName)

  override def beforeAll(): Unit = {
    FileWithExt.delete()
    FileWithoutExt.delete()
    val schema: Schema = new Schema.Parser().parse(OrderSchema)
    val genericRecord = new GenericData.Record(schema)
    genericRecord.put("id", 1)
    genericRecord.put("amount", 39.55d)

    Seq(FileWithExt, FileWithoutExt).foreach(fileToSave => {
      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val fileWriter = new DataFileWriter[GenericRecord](datumWriter)
      fileWriter.create(schema, fileToSave)
      fileWriter.append(genericRecord)
      fileWriter.close()

    })
  }

  override def afterAll(): Unit = {
    FileWithExt.delete()
    FileWithoutExt.delete()
  }

  private val TestedSparkSession: SparkSession = SparkSession.builder()
    .appName("Higher-order test").master("local[*]").getOrCreate()
  import TestedSparkSession.implicits._

  behavior of "Avro data source"

  it should "read Apache Avro data not written by Apache Spark SQL" in {
    val orders = TestedSparkSession.read.format("avro").load(FileWithExt.getAbsolutePath)

    val collectedOrders = orders.collect().map(row => s"${row.getAs[Int]("id")}: ${row.getAs[Double]("amount")}")
    collectedOrders should have size 1
    collectedOrders(0) shouldEqual "1: 39.55"
  }

  it should "fail on reading Avro with ignoreExtension disabled" in {
    val fileWithoutExtension = new File("./sample_avro")
    fileWithoutExtension.deleteOnExit()
    fileWithoutExtension.createNewFile()

    val exception = intercept[FileNotFoundException] {
      val orders = TestedSparkSession.read.format("avro").option("ignoreExtension", false).load(fileWithoutExtension.getAbsolutePath)
      orders.show(3)
    }

    exception.getMessage should include("No Avro files found. If files don't have .avro extension, set ignoreExtension to true")
  }

  it should "read the data written by Avro writer" in {
    val savedAvroDataset = new File("./avro_dataset.avro")
    savedAvroDataset.deleteOnExit()
    val orders = Seq((1L, "order#1"), (2L, "order#2")).toDF("id", "label")

    orders.write.format("avro").mode(SaveMode.Overwrite).save(savedAvroDataset.getAbsolutePath)

    val ordersFromWrittenAvro = TestedSparkSession.read.format("avro").load(savedAvroDataset.getAbsolutePath)

    val collectedOrders = ordersFromWrittenAvro.collect().map(row => s"${row.getAs[Long]("id")}: ${row.getAs[String]("label")}")
    collectedOrders should have size 2
    collectedOrders should contain allOf("1: order#1", "2: order#2")
  }

  it should "write data with customized Avro options" in {
    val savedAvroDataset = new File("./avro_dataset")
    val orders = Seq((1, 20.99d)).toDF("id", "amount")

    orders.write.format("avro").mode(SaveMode.Overwrite).option("recordName", "orderFromSpark")
      .option("recordNamespace", "com.waitingforcode.spark.avro").option("compression", "bzip2")
      .save(savedAvroDataset.getAbsolutePath)

    val dataFile = savedAvroDataset.listFiles().toSeq.find(file => file.getName.startsWith("part")).get
    val datuumReader = new GenericDatumReader[GenericData.Record]()
    val dataFileReader = new DataFileReader[GenericData.Record](dataFile, datuumReader)
    val readOrder = dataFileReader.next()
    readOrder.toString shouldEqual "{\"id\": 1, \"amount\": 20.99}"
    datuumReader.getSchema.getName shouldEqual "orderFromSpark"
    datuumReader.getSchema.getNamespace shouldEqual "com.waitingforcode.spark.avro"
    dataFileReader.getMetaString("avro.codec") shouldEqual "bzip2"
  }

  it should "fail on reading Avro data with mismatching schema" in {
    val mismatchingSchema = OrderSchema.replace("amount", "amount_mismatch")

    val exception = intercept[SparkException] {
      TestedSparkSession.read.format("avro").option("avroSchema", mismatchingSchema).load(FileWithoutExt.getAbsolutePath).count()
    }

    exception.getMessage should include("Found order, expecting order, missing required field amount_mismatch")
  }

}