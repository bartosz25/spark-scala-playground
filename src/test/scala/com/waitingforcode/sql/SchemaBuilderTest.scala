package com.waitingforcode.sql

import com.waitingforcode.sql.fields.{array, integer, newStruct, string}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SchemaBuilderTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  behavior of "book schema"

  it should "be defined manually" in {
    val bookSchema = StructType(
      Array(
        StructField("title", StringType, false),
        StructField("authors",
          ArrayType(StructType(Array(StructField("first_name", StringType, false),
            StructField("last_name", StringType, false))), false), false
        ),
        StructField("metadata", StructType(Array(
          StructField("tags", ArrayType(StringType, false), true),
          StructField("categories", StructType(Array(
            StructField("name", StringType, false), StructField("books_number", IntegerType, false)
          )), true)
        )), true)
      )
    )

    bookSchema.toDDL shouldEqual "`title` STRING,`authors` ARRAY<STRUCT<`first_name`: STRING, `last_name`: STRING>>," +
      "`metadata` STRUCT<`tags`: ARRAY<STRING>, `categories`: STRUCT<`name`: STRING, `books_number`: INT>>"
  }

  it should "be defined with an alternative builder" in {
    val schema = new StructBuilder()
      .withRequiredFields(Map(
        "title" -> string, "authors" -> array(
          newStruct.withOptionalFields(Map("first_name" -> string, "last_name" -> string)).buildSchema, false
        )
      ))
      .withOptionalFields(Map(
        "metadata" -> newStruct.withRequiredFields(Map(
          "tags" -> array(StringType, false),
          "categories" -> newStruct.withOptionalFields(Map("name" -> string, "books_number" -> integer)).toField
        )).toField
      )).buildSchema

    schema.toDDL shouldEqual "`metadata` STRUCT<`tags`: ARRAY<STRING>, `categories`: STRUCT<`name`: STRING, " +
      "`books_number`: INT>>,`title` STRING,`authors` ARRAY<STRUCT<`first_name`: STRING, `last_name`: STRING>>"
  }

}



class StructBuilder() {

  private var structFields = Array.empty[StructField]

  def withRequiredFields = withFields(false) _

  def withOptionalFields = withFields(true) _

  private def withFields(nullable: Boolean)(creators: Map[String, (String, Boolean) => StructField]) = {
    val mappedFields = creators.map {
      case (fieldName, fieldGenerator) => fieldGenerator(fieldName, nullable)
    }
    structFields = mappedFields.toArray ++ structFields
    this
  }

  def build: Array[StructField] = structFields

  def buildSchema: StructType = StructType(structFields)

  def toField = fields.struct(structFields)
}

object fields {
  private def baseField(fieldType: DataType)(fieldName: String, nullable: Boolean) =
    new StructField(fieldName, fieldType, nullable)
  def string = baseField(StringType) _
  def array(fieldType: DataType, nullableContent: Boolean) = baseField(ArrayType(fieldType, nullableContent)) _
  def struct(fields: Array[StructField]) = baseField(StructType(fields)) _
  def long = baseField(LongType) _
  def integer = baseField(IntegerType) _
  def double = baseField(DoubleType) _

  def newStruct = new StructBuilder()

}

