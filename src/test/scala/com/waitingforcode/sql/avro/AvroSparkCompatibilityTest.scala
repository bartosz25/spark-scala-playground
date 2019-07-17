package com.waitingforcode.sql.avro

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.util

import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.reflect.ReflectDatumWriter
import org.apache.avro.{AvroTypeException, Schema}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class AvroSparkCompatibilityTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val TestedSparkSession: SparkSession = SparkSession.builder()
    .appName("Avro-Spark compatibility test").master("local[*]").getOrCreate()
  import TestedSparkSession.implicits._

  private val orderSchema = StructType(
    Seq(
      StructField("id", LongType, false),
      StructField("creationDate", StringType, false),
      StructField("promotionalCode", IntegerType, true),
      StructField("products", ArrayType(
        StructType(
          Seq(
            StructField("id", LongType, false), StructField("name", StringType, false),
            StructField("unitaryPrice", DoubleType, false), StructField("productsInBasket", IntegerType, false)
          )
        ), false
      ), false),
      StructField("extraInfo", MapType(StringType, StringType), false),
      StructField("customer", StructType(
        Seq(
          StructField("customerType", StringType, false),
          StructField("address", StructType(
            Seq(
              StructField("postalCode", IntegerType, false),
              StructField("city", StringType, false)
            )
          ), false)
        )
      ), false)
    )
  )

  private val baseDir = s"/tmp/avro_tests"
  new File(baseDir).mkdir()
  private val schemaFile = new File(s"${baseDir}/avro_schema.json")
  Files.copy(getClass.getResourceAsStream("/avro_compatibility/avro_schema.json"), schemaFile.toPath,
    StandardCopyOption.REPLACE_EXISTING)
  private val schemaJson = FileUtils.readFileToString(schemaFile)
  private val avroSchema = new Schema.Parser().parse(schemaFile)

  "Avro Apache Spark reader" should "read files produced by Avro Java writer" in {
    val order1 = Order.valueOf(100L, "2019-05-10T05:55:00.093Z", null, util.Arrays.asList(
      Product.valueOf(1L, "milk", 0.99d, 1)
    ), new util.HashMap[String, String](), new Customer(CustomerTypes.BUSINESS, new Customer.Address(33000, "Bordeaux")))
    val extraInfo2 = new util.HashMap[String, String]()
    extraInfo2.put("first_order", "true")
    extraInfo2.put("bonus_points", "30")
    val order2 = Order.valueOf(101L, "2019-05-10T15:55:00.093Z", 123, util.Arrays.asList(
      Product.valueOf(1L, "milk", 0.99d, 2), Product.valueOf(2L, "coffee", 3.99d, 4),
      Product.valueOf(3L, "chocolate", 1.99d, 2)
    ), extraInfo2, new Customer(CustomerTypes.PRIVATE, new Customer.Address(75001, "Paris")))
    val datumWriter = new ReflectDatumWriter[Order](avroSchema)
    val dataFileWriter = new DataFileWriter[Order](datumWriter)
    val inputFilePath = s"${baseDir}/test_avro_writer_spark_reader.avro"
    new File(inputFilePath).delete()
    dataFileWriter.create(avroSchema, new File(inputFilePath))
    Seq(order1, order2).foreach(order => dataFileWriter.append(order))
    dataFileWriter.close()

    val orders = TestedSparkSession.read.format("avro").load(inputFilePath)
      .map(row => row.mkString("; "))
      .collect()

    orders should have size 2
    orders should contain allOf("100; 2019-05-10T05:55:00.093Z; null; WrappedArray([1,milk,0.99,1]); " +
      "Map(); [BUSINESS,[33000,Bordeaux]]",
    "101; 2019-05-10T15:55:00.093Z; 123; WrappedArray([1,milk,0.99,2], [2,coffee,3.99,4], [3,chocolate,1.99,2]); " +
      "Map(first_order -> true, bonus_points -> 30); [PRIVATE,[75001,Paris]]")
  }

  "the first Spark to Avro test" should "fail when reading created Avro files" in {
    val ordersDataset = Seq(
      ScalaOrder(102L, "2019-05-10T13:55:00.093Z", None, Seq(
        ScalaProduct(1L, "milk", 0.99d, 2), ScalaProduct(2L, "coffee", 3.99d, 4)
      ), Map.empty, ScalaCustomer("PRIVATE", ScalaCustomerAddress(39000, "xx"))),
      ScalaOrder(101L, "2019-05-10T15:55:00.093Z" , Some(123), Seq(
        ScalaProduct(1L, "milk", 0.99d, 2), ScalaProduct(2L, "coffee", 3.99d, 4)
      ), Map.empty, ScalaCustomer("PRIVATE", ScalaCustomerAddress(39000, "xx")))
    ).toDF
    val outputDir = s"${baseDir}/spark_producer_failure_1"

    ordersDataset.repartition(1).write.format("avro").mode(SaveMode.Overwrite)
      .save(outputDir)

    intercept[AvroTypeException] {
      new File(outputDir).listFiles.toSeq.filter(file => file.getName.endsWith(".avro"))
        .foreach(avroFile => {
          val genericReader = new GenericDatumReader[GenericData.Record](avroSchema)
          val dataFileReader = DataFileReader.openReader(avroFile, genericReader)
          // Consume just one record to see if it works
          dataFileReader.next()
        })
    }
  }

  "the second Spark to Avro test" should "fail even writing files with schema" in {
    val ordersDataset = Seq(
      ScalaOrder(102L, "2019-05-10T13:55:00.093Z", None, Seq(
        ScalaProduct(1L, "milk", 0.99d, 2), ScalaProduct(2L, "coffee", 3.99d, 4)
      ), Map.empty, ScalaCustomer("PRIVATE", ScalaCustomerAddress(39000, "xx"))),
      ScalaOrder(101L, "2019-05-10T15:55:00.093Z" , Some(123), Seq(
        ScalaProduct(1L, "milk", 0.99d, 2), ScalaProduct(2L, "coffee", 3.99d, 4)
      ), Map.empty, ScalaCustomer("PRIVATE", ScalaCustomerAddress(39000, "xx")))
    ).toDF
    val outputDir = s"${baseDir}/spark_producer_failure_2"

    ordersDataset.schema
    intercept[SparkException] {
      ordersDataset.repartition(1).write.format("avro").option("avroSchema", schemaJson)
        .mode(SaveMode.Overwrite)
        .save(outputDir)
    }
  }

  "the third Spark to Avro test" should "work with explicit schema" in {
    val ordersDataset = Seq(
      ScalaOrder(102L, "2019-05-10T13:55:00.093Z", None, Seq(
        ScalaProduct(1L, "milk", 0.99d, 2), ScalaProduct(2L, "coffee", 3.99d, 4)
      ), Map.empty, ScalaCustomer("PRIVATE", ScalaCustomerAddress(11000, "xx"))),
      ScalaOrder(101L, "2019-05-10T15:55:00.093Z" , Some(123), Seq(
        ScalaProduct(1L, "milk", 0.99d, 1), ScalaProduct(2L, "coffee", 3.99d, 4)
      ), Map.empty, ScalaCustomer("PRIVATE", ScalaCustomerAddress(39000, "xx")))
    ).toDF
    val outputDir = s"${baseDir}/spark_producer_success_with_schema"
    val ordersWithExplicitSchema = ordersDataset.sparkSession.createDataFrame(ordersDataset.rdd, orderSchema)

    ordersWithExplicitSchema.repartition(1).write.format("avro")
      .option("avroSchema", schemaJson).mode(SaveMode.Overwrite)
      .save(outputDir)

    val records = new File(outputDir).listFiles.toSeq.filter(file => file.getName.endsWith(".avro"))
      .flatMap(avroFile => {
        val genericReader = new GenericDatumReader[GenericData.Record](avroSchema)
        val dataFileReader = DataFileReader.openReader(avroFile, genericReader)
        import scala.collection.JavaConverters._
        dataFileReader.iterator().asScala
      })

    records should have size 2
    records.map(genericRecord => genericRecord.get("id")) should contain allOf(101, 102)
    records.map(genericRecord => genericRecord.get("creationDate").toString) should contain allOf(
      "2019-05-10T13:55:00.093Z", "2019-05-10T15:55:00.093Z"
    )
    records.map(genericRecord => genericRecord.get("promotionalCode")) should contain allOf(null, 123)
    records.map(genericRecord => genericRecord.get("products").toString) should contain allOf(
      "[{\"id\": 1, \"name\": \"milk\", \"unitaryPrice\": 0.99, \"productsInBasket\": 2}, {\"id\": 2, \"name\": \"coffee\", \"unitaryPrice\": 3.99, \"productsInBasket\": 4}]",
      "[{\"id\": 1, \"name\": \"milk\", \"unitaryPrice\": 0.99, \"productsInBasket\": 1}, {\"id\": 2, \"name\": \"coffee\", \"unitaryPrice\": 3.99, \"productsInBasket\": 4}]"
    )
    records.map(genericRecord => genericRecord.get("extraInfo").toString) should contain only("{}")
    records.map(genericRecord => genericRecord.get("customer").toString) should contain allOf(
      "{\"customerType\": \"PRIVATE\", \"address\": {\"postalCode\": 11000, \"city\": \"xx\"}}",
      "{\"customerType\": \"PRIVATE\", \"address\": {\"postalCode\": 39000, \"city\": \"xx\"}}"
    )
  }

  "Avro conversion" should "fail for JSON file" in {
    val extraOrders =
      """
        |{"id": 200, "creationDate": "2019-04-10T19:55:00.093Z", "promotionalCode": null, "products": [{"id": 5, "name": "water", "unitaryPrice": 0.49, "productsInBasket": 3}, {"id": 6, "name": "sugar", "unitaryPrice": 0.99, "productsInBasket": 1}], "extraInfo": {"add_notice": "true"}, "customer": {"customerType": "PRIVATE", "address": {"postalCode": 57000, "city": "Metz"}}}
        |{"id": 201, "creationDate": "2019-04-10T11:55:00.093Z", "promotionalCode": null, "products": [{"id": 5, "name": "water", "unitaryPrice": 0.49, "productsInBasket": 3}, {"id": 6, "name": "sugar", "unitaryPrice": 0.99, "productsInBasket": 1}], "extraInfo": {"add_notice": "true"}, "customer": {"customerType": "PRIVATE", "address": {"postalCode": 57000, "city": "Metz"}}}
      """.stripMargin
    val ordersToAddFile = s"${baseDir}/extra_avro_data.json"
    FileUtils.writeStringToFile(new File(ordersToAddFile), extraOrders, false)
    val ordersFromJson = TestedSparkSession.read.schema(orderSchema)
      .option("mode", "permissive")
      .json(ordersToAddFile)

    ordersFromJson.printSchema()
    ordersFromJson.show()

    intercept[SparkException] {
      ordersFromJson.repartition(1).write.option("avroSchema", schemaJson)
        .format("avro").mode(SaveMode.Overwrite)
        .save(s"${baseDir}/failure_json")
    }
  }

}

case class ScalaOrder(id: Long, creationDate: String, promotionalCode: Option[Int], products: Seq[ScalaProduct],
                      extraInfo: Map[String, String], customer: ScalaCustomer)
case class ScalaProduct(id: Long, name: String, unitaryPrice: Double, productsInBasket: Int)
case class ScalaCustomer(customerType: String, address: ScalaCustomerAddress)
case class ScalaCustomerAddress(postalCode: Int, city: String)