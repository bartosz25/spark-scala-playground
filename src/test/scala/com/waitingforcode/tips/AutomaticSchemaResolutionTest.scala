package com.waitingforcode.tips

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}

class AutomaticSchemaResolutionTest extends FlatSpec with Matchers {

  "schema" should "be resolved automatically through reflection" in {
    val schema = ScalaReflection.schemaFor[Alphabet].dataType.asInstanceOf[StructType]

    schema.fields should have size 3
    schema.fields should contain allOf(StructField("letter1", StringType, true),
      StructField("letter2", StringType, true), StructField("otherLetters", ArrayType(StringType, true), true))
  }

}

case class Alphabet(letter1: String, letter2: String, otherLetters: Seq[String])
