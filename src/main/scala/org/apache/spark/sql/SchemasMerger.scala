package org.apache.spark.sql

import org.apache.spark.sql.types.StructType

object SchemasMerger {

  def merge(schema1: StructType, schema2: StructType): StructType = {
    schema1.merge(schema2)
  }

}
