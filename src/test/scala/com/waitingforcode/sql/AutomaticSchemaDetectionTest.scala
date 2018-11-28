package com.waitingforcode.sql

import java.io.File

import com.waitingforcode.util.InMemoryDatabase
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class AutomaticSchemaDetectionTest extends FlatSpec with Matchers with BeforeAndAfter {

  var sparkSession: SparkSession = null
  val structuredFile = new File("./structured_data.json")
  val semiStructuredFile = new File("./semi_structured_data.json")
  val structuredParquetFile = new File("./structured_data.parquet")

  before {
    InMemoryDatabase.cleanDatabase()
    sparkSession = SparkSession.builder()
      .appName("Spark SQL automatic schema resolution test").master("local[1]").getOrCreate()
    // Structured data
    val structuredEntries =
      """
        | {"name": "A", "age": 20, "hobby": null}
        | {"name": "B", "age": 24, "hobby": "sport"}
        | {"name": "C", "age": 30, "hobby": "travelling"}
        | {"name": "D", "age": 34, "hobby": null}
        | {"name": "E", "age": 40, "hobby": "cooking"}
        | {"name": "F", "age": 44, "hobby": null}
      """.stripMargin
    FileUtils.writeStringToFile(structuredFile, structuredEntries)
    // Semi-structured data (in some cases, keys are missing)
    val semiStructuredEntries =
      """
        | {"name": "A", "age": 20, "hobby": null}
        | {"name": "B", "city": "London", "hobby": null}
        | {"name": "C", "country": "France"}
        | {"name": "D", "age": 20, "hobby": null}
        | {"preferences": {"sport": "football", "fruit": "apple"}}
        | {"job": "programmer"}
        | {"age": 20, "job": "software engineer"}
      """.stripMargin
    FileUtils.writeStringToFile(semiStructuredFile, semiStructuredEntries)
  }

  after {
    FileUtils.forceDelete(structuredFile)
    FileUtils.forceDelete(semiStructuredFile)
    FileUtils.forceDelete(structuredParquetFile)
    sparkSession.stop()
  }

  "structured data" should "have its schema resolved automatically" in {
    val structuredJsonData = sparkSession.read.json(structuredFile.getAbsolutePath)

    val resolvedSchema = structuredJsonData.schema
    val schemaFields = resolvedSchema.fields

    val nameField = schemaFields.filter(field => field.name == "name")(0)
    nameField.dataType shouldBe(DataTypes.StringType)
    nameField.nullable shouldBe true
    val ageField = schemaFields.filter(field => field.name == "age")(0)
    ageField.dataType shouldBe(DataTypes.LongType)
    ageField.nullable shouldBe true
    val hobbyField = schemaFields.filter(field => field.name == "hobby")(0)
    hobbyField.dataType shouldBe(DataTypes.StringType)
    hobbyField.nullable shouldBe true
  }

  "structured data from in-memory database" should "have the schema resolved automatically" in {
    InMemoryDatabase
      .createTable("CREATE TABLE city(name varchar(30) NOT NULL, country varchar(50))")
    val structuredDbData = sparkSession.read.format("jdbc")
      .option("url", InMemoryDatabase.DbConnection)
      .option("driver", InMemoryDatabase.DbDriver)
      .option("dbtable", "city")
      .option("user", InMemoryDatabase.DbUser)
      .option("password", InMemoryDatabase.DbPassword)
      .load()

    val resolvedSchema = structuredDbData.schema
    val schemaFields = resolvedSchema.fields

    // TODO : here the nameField is set to nullable
    println(schemaFields.mkString("\n"))
    val nameField = schemaFields.filter(field => field.name == "NAME")(0)
    nameField.dataType shouldBe(DataTypes.StringType)
    nameField.nullable shouldBe false
    val countryField = schemaFields.filter(field => field.name == "COUNTRY")(0)
    countryField.dataType shouldBe(DataTypes.StringType)
    countryField.nullable shouldBe true
  }

  "Parquet files" should "also have their schemas resolved automatically" in {
    val structuredJsonData = sparkSession.read.json(structuredFile.getAbsolutePath)
    structuredJsonData.write.parquet(structuredParquetFile.getAbsolutePath)
    val structuredParquetData = sparkSession.read.parquet(structuredParquetFile.getAbsolutePath)

    val resolvedSchema = structuredParquetData.schema
    val schemaFields = resolvedSchema.fields

    // The schema is expected to be the same as in the case
    // of test on structured JSON
    val nameField = schemaFields.filter(field => field.name == "name")(0)
    nameField.dataType shouldBe(DataTypes.StringType)
    nameField.nullable shouldBe true
    val ageField = schemaFields.filter(field => field.name == "age")(0)
    ageField.dataType shouldBe(DataTypes.LongType)
    ageField.nullable shouldBe true
    val hobbyField = schemaFields.filter(field => field.name == "hobby")(0)
    hobbyField.dataType shouldBe(DataTypes.StringType)
    hobbyField.nullable shouldBe true
  }

  "low sampling" should "make select query fail because of not detected column" in {
    // This time we defined a low sampling ratio
    // Because of that Spark SQL will analyze less
    // JSON entries and, probably, resolve schema for
    // only a part of columns; Thus some of columns
    // can be missing
    val semiStructuredJsonData = sparkSession.read.option("samplingRatio", 0.1)
      .json(semiStructuredFile.getAbsolutePath)

    // Note: if the test fails, it means that 'preferences' was
    //       resolved but there are some other columns missing
    //       To facilitate test understanding, 'preferences' was chosen
    //       as it was never found in local tests.
    val queryError = intercept[AnalysisException] {
      semiStructuredJsonData.select("preferences")
    }

    queryError.message.contains("cannot resolve '`preferences`'") shouldBe true
  }

  "high sampling" should "resolve all columns correctly" in {
    // Please note that by default the sampling ratio
    // is equal to 1.0. Here it's repeated only to
    // make insight on parameters difference between
    // both tests
    val semiStructuredJsonData = sparkSession.read.option("samplingRatio", 1.0).
      json(semiStructuredFile.getAbsolutePath)

    // Unlike previously, this query shouldn't fail because
    // all JSON entries were analyzed and 'preferences' entry
    // was found and included in the schema
    semiStructuredJsonData.select("preferences")

    val resolvedSchema = semiStructuredJsonData.schema
    val fields = resolvedSchema.fields

    val ageField = fields.filter(field => field.name == "age")(0)
    ageField.dataType shouldEqual(DataTypes.LongType)
    val cityField = fields.filter(field => field.name == "city")(0)
    cityField.dataType shouldEqual(DataTypes.StringType)
    val countryField = fields.filter(field => field.name == "country")(0)
    countryField.dataType shouldEqual(DataTypes.StringType)
    val hobbyField = fields.filter(field => field.name == "hobby")(0)
    hobbyField.dataType shouldEqual(DataTypes.StringType)
    val jobField = fields.filter(field => field.name == "job")(0)
    jobField.dataType shouldEqual(DataTypes.StringType)
    val nameField = fields.filter(field => field.name == "name")(0)
    nameField.dataType shouldEqual(DataTypes.StringType)
    val preferencesField = fields.filter(field => field.name == "preferences")(0)
    val expectedPreferencesType = StructType(Seq(
      StructField("fruit",DataTypes.StringType,true),
      StructField("sport",DataTypes.StringType,true)
    ))
    preferencesField.dataType shouldEqual(expectedPreferencesType)
  }

}
