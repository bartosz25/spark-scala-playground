package com.waitingforcode.sql

import java.sql.Timestamp

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, SparkSession, functions}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class SlowlyChangingDimensionsTest extends FlatSpec with Matchers {

  private val testSparkSession: SparkSession = SparkSession.builder().appName("Deequ test").master("local[*]")
    .getOrCreate()
  import testSparkSession.implicits._
  private val prefixDir = "/tmp/slowly-changing-dimensions"

  behavior of "slowly changing dimensions"

  it should "overwrite changes in type 1" in {
    val currentDataset = Seq(
      Providers(1L, "provider A", "Big Data"), Providers(2L, "provider B", "cloud"),
      Providers(3L, "provider C", "software engineering"), Providers(4L, "provider D", "unknown")
    ).toDF()
    val newDataset = Seq(
      Providers(4L, "provider D", "QA")
    ).toDF()
    def aliasColumns(prefix: String, schema: StructType): Seq[Column] = {
      schema.fields.map(field => functions.col(field.name).as(s"${prefix}${field.name}"))
    }


    val currentDatasetAliases = currentDataset.select(
      aliasColumns("current_", currentDataset.schema): _*
    )
    val newDatasetAliased = newDataset.select(
      aliasColumns("new_", currentDataset.schema): _*
    )

    def generateMapping(schema: StructType): Seq[Column] = {
      schema.fields.map(field => {
        functions.when(functions.col(s"new_${field.name}").isNull, functions.col(s"current_${field.name}"))
          .otherwise(functions.col(s"new_${field.name}")).as(s"${field.name}")
      })
    }

    // I'm doing the overwrite with a custom SELECT but you can also
    // do it in .map(...) function with only 1 if-else condition:
    // `if row.getAs[String]("new_name") != null ==> map to the new
    //  otherwise map to the old. Of course, it will only work when
    // all columns, even the ones that didn't change, are defined in the
    // new dataset.
    val mappedProviders = currentDatasetAliases
      .join(newDatasetAliased, $"current_id" === $"new_id", "full_outer")
      .select(generateMapping(currentDataset.schema): _*).as[Providers]
      .collect()

    mappedProviders should have size 4
    mappedProviders should contain allOf(
      Providers(1L, "provider A", "Big Data"), Providers(2L, "provider B", "cloud"),
      Providers(3L, "provider C", "software engineering"), Providers(4L, "provider D", "QA")
    )
  }

  it should "create new active flag for type 2" in {
    val currentDataset = Seq(
      ProvidersType2(1L, "provider A", "software", Some(new Timestamp(0L)), Some(new Timestamp(1L)), Some("N")),
      ProvidersType2(1L, "provider A", "software & Big Data", Some(new Timestamp(1L)), Some(new Timestamp(3L)), Some("N")),
      ProvidersType2(1L, "provider A", "Big Data", Some(new Timestamp(3L)), None, Some("Y")),
      ProvidersType2(2L, "provider B", "cloud", Some(new Timestamp(3L)), None, Some("Y")),
      ProvidersType2(3L, "provider C", "software engineering", Some(new Timestamp(3L)), None, Some("Y")),
      ProvidersType2(4L, "provider D", "unknown", Some(new Timestamp(3L)), None, Some("Y"))
    ).toDF()
    val newDataset = Seq(
      ProvidersType2(4L, "provider D", "QA"),
      ProvidersType2(5L, "provider E", "data science")
    ).toDF()

    val changeDateMillis = 5L
    val broadcastedChangeDateMillis = testSparkSession.sparkContext.broadcast(changeDateMillis)

    val mappedProviders = currentDataset.union(newDataset).as[ProvidersType2]
      .groupByKey(provider => {
        if (provider.startDate.isEmpty || provider.currentFlag.getOrElse("") == "Y") {
          s"possible_change-${provider.id}"
        } else {
          provider.id.toString
        }
      })
      .flatMapGroups {
        case (key, records) => {
          if (key.startsWith("possible_change")) {
            val mappedRecords = new mutable.ListBuffer[ProvidersType2]()
            var shouldOverwrite = false
            while (records.hasNext) {
              // The logic behind this is straightforward.
              // We suppose that we'll have at most 2 records in this group because
              // at a given moment we can only have 1 active row and, eventually,
              // 1 candidate row. If the latter happens, the `shouldOverwrite` will be true
              // and therefore, we'll need to change the state of every row accordingly

              // Another solution would be to group by the `id` property and materialize the whole
              // dataset at once but I tried to do it "smart way", without the data materialization
              val recordToMap = records.next()
              shouldOverwrite = (shouldOverwrite || records.hasNext)
              if (shouldOverwrite) {
                mappedRecords.append(recordToMap.transitToNewState(broadcastedChangeDateMillis.value))
              } else {
                // If we have only 1 row here, it means either that it didn't change or
                // that it's the new row. `enableIfDisabled` will do nothing
                // for the former case. It will only promote the new row
                // as the currently used.
                mappedRecords.append(recordToMap.enableIfDisabled(broadcastedChangeDateMillis.value))
              }
            }
            mappedRecords
          } else {
            records
          }
        }
      }.collect()


    mappedProviders should have size 8
    mappedProviders should contain allOf(
      ProvidersType2(1L, "provider A", "software", Some(new Timestamp(0L)), Some(new Timestamp(1L)), Some("N")),
      ProvidersType2(1L, "provider A", "software & Big Data", Some(new Timestamp(1L)), Some(new Timestamp(3L)), Some("N")),
      ProvidersType2(1L, "provider A", "Big Data", Some(new Timestamp(3L)), None, Some("Y")),
      ProvidersType2(2L, "provider B", "cloud", Some(new Timestamp(3L)), None, Some("Y")),
      ProvidersType2(3L, "provider C", "software engineering", Some(new Timestamp(3L)), None, Some("Y")),
      ProvidersType2(4L, "provider D", "unknown", Some(new Timestamp(3L)), Some(new Timestamp(5L)), Some("N")),
      ProvidersType2(4L, "provider D", "QA", Some(new Timestamp(5L)), None, Some("Y")),
      ProvidersType2(5L, "provider E", "data science", Some(new Timestamp(5L)), None, Some("Y"))
    )
  }

  it should "switch new/old columns for type 3" in {
    val currentDataset = Seq(
      ProvidersType3(1L, "provider A", None, "Big Data", None),
      ProvidersType3(2L, "provider B", None, "cloud", None),
      ProvidersType3(3L, "provider C", None, "software engineering", None),
      ProvidersType3(4L, "provider D", None, "unknown", None)
    ).toDF()
    val newDataset = Seq(
      ProvidersType3(4L, "provider D", None, "QA", None),
      ProvidersType3(5L, "provider E", None, "DevOps", None) // This will be our new row
    ).toDF()
    def aliasColumns(prefix: String, schema: StructType): Seq[Column] = {
      schema.fields.map(field => functions.col(field.name).as(s"${prefix}${field.name}"))
    }
    val currentDatasetAliases = currentDataset.select(
      aliasColumns("current_", currentDataset.schema): _*
    )
    val newDatasetAliased = newDataset.select(
      aliasColumns("new_", currentDataset.schema): _*
    )

    def generateMapping(staticColumns: Seq[String], columnsToSwitch: Seq[String]): Seq[Column] = {
      val staticColumnNames = staticColumns.map(field => {
        functions.when(functions.col(s"new_${field}").isNull, functions.col(s"current_${field}"))
          .otherwise(functions.col(s"new_${field}")).as(s"${field}")
      })
      val currentNames = columnsToSwitch.map(field => {
        functions
          .when(functions.col(s"new_${field}").isNotNull, functions.col(s"new_${field}"))
          .otherwise(functions.col(s"current_${field}")).as(s"${field}")
      })
      val previousNames = columnsToSwitch.map(field => {
        functions
          .when(functions.col(s"new_${field}").isNotNull, functions.col(s"current_${field}"))
          .otherwise(functions.col(s"current_previous${field.capitalize}")).as(s"previous${field.capitalize}")
      })
      staticColumnNames ++ currentNames ++ previousNames
    }

    val mappedProviders = currentDatasetAliases
      .join(newDatasetAliased, $"current_id" === $"new_id", "full_outer")
      .select(
        generateMapping(Seq("id"), Seq("name", "specialty")): _*).as[ProvidersType3]
      .collect()

    mappedProviders should have size 5
    mappedProviders should contain allOf(
      ProvidersType3(1, "provider A", None, "Big Data",None),
      ProvidersType3(2, "provider B", None, "cloud",None),
      ProvidersType3(3, "provider C", None, "software engineering", None),
      ProvidersType3(4, "provider D", Some("provider D"), "QA" , Some("unknown")),
      ProvidersType3(5, "provider E", None, "DevOps", None)
    )
  }

  it should "add new row and update validity period for type 4" in {
    val currentDataset = Seq(
      ProvidersType4(1L, "provider A", "software", Some(new Timestamp(0L)), Some(new Timestamp(1L))),
      ProvidersType4(1L, "provider A", "software & Big Data", Some(new Timestamp(1L)), Some(new Timestamp(3L))),
      ProvidersType4(1L, "provider A", "Big Data", Some(new Timestamp(3L)), None),
      ProvidersType4(2L, "provider B", "cloud", Some(new Timestamp(3L)), None),
      ProvidersType4(3L, "provider C", "software engineering", Some(new Timestamp(3L)), None),
      ProvidersType4(4L, "provider D", "unknown", Some(new Timestamp(3L)), None)
    ).toDF()
    val newDataset = Seq(
      ProvidersType4(4L, "provider D", "QA"),
      ProvidersType4(5L, "provider E", "backend engineering")
    ).toDF()

    val changeDateMillis = 5L
    val broadcastedChangeDateMillis = testSparkSession.sparkContext.broadcast(changeDateMillis)

    val mappedProviders = currentDataset.union(newDataset).as[ProvidersType4]
      .groupByKey(provider => {
        // Handles both, new and old active row
        if (provider.endDate.isEmpty) {
          s"possible_change-${provider.id}"
        } else {
          provider.id.toString
        }
      })
      .flatMapGroups {
        case (key, records) => {
          if (key.startsWith("possible_change")) {
            val mappedRecords = new mutable.ListBuffer[ProvidersType4]()
            var shouldOverwrite = false
            while (records.hasNext) {
              val recordToMap = records.next()
              shouldOverwrite = (shouldOverwrite || records.hasNext)
              if (shouldOverwrite) {
                mappedRecords.append(recordToMap.transitToNewState(broadcastedChangeDateMillis.value))
              } else {
                mappedRecords.append(recordToMap.enableIfDisabled(broadcastedChangeDateMillis.value))
              }
            }
            mappedRecords
          } else {
            records
          }
        }
      }.collect()


    mappedProviders should have size 8
    mappedProviders should contain allOf(
      ProvidersType4(1L, "provider A", "software", Some(new Timestamp(0L)), Some(new Timestamp(1L))),
      ProvidersType4(1L, "provider A", "software & Big Data", Some(new Timestamp(1L)), Some(new Timestamp(3L))),
      ProvidersType4(1L, "provider A", "Big Data", Some(new Timestamp(3L)), None),
      ProvidersType4(2L, "provider B", "cloud", Some(new Timestamp(3L)), None),
      ProvidersType4(3L, "provider C", "software engineering", Some(new Timestamp(3L)), None),
      ProvidersType4(4L, "provider D", "unknown", Some(new Timestamp(3L)), Some(new Timestamp(5L))),
      ProvidersType4(4L, "provider D", "QA", Some(new Timestamp(5L)), None),
      ProvidersType4(5L, "provider E", "backend engineering", Some(new Timestamp(5L)), None)
    )
  }

}

case class Providers(id: Long, name: String, specialty: String)
case class ProvidersType2(id: Long, name: String, specialty: String,
                          startDate: Option[Timestamp] = None, endDate: Option[Timestamp] = None, currentFlag: Option[String] = None) {
  def transitToNewState(dateTimeMillis: Long): ProvidersType2 = {
    if (currentFlag == ProvidersType2.EnabledFlag) {
      disable(dateTimeMillis)
    } else {
      enable(dateTimeMillis)
    }
  }

  def disable(dateTimeMillis: Long): ProvidersType2 = {
    this.copy(endDate = Some(new Timestamp(dateTimeMillis)), currentFlag = ProvidersType2.DisabledFlag)
  }

  def enable(dateTimeMillis: Long): ProvidersType2 = {
    this.copy(startDate = Some(new Timestamp(dateTimeMillis)), currentFlag = ProvidersType2.EnabledFlag)
  }

  def enableIfDisabled(dateTimeMillis: Long): ProvidersType2 = {
    if (currentFlag.isEmpty) {
      this.copy(startDate = Some(new Timestamp(dateTimeMillis)), currentFlag = ProvidersType2.EnabledFlag)
    } else {
      this
    }
  }
}
object ProvidersType2 {
  val EnabledFlag = Some("Y")
  val DisabledFlag = Some("N")
}

case class ProvidersType3(id: Long, name: String, previousName: Option[String] = None,
                          specialty: String, previousSpecialty: Option[String] = None)

case class ProvidersType4(id: Long, name: String, specialty: String,
                          startDate: Option[Timestamp] = None, endDate: Option[Timestamp] = None) {
  def transitToNewState(dateTimeMillis: Long): ProvidersType4 = {
    if (startDate.nonEmpty && endDate.isEmpty) {
      disable(dateTimeMillis)
    } else {
      enable(dateTimeMillis)
    }
  }

  def disable(dateTimeMillis: Long): ProvidersType4 = {
    this.copy(endDate = Some(new Timestamp(dateTimeMillis)))
  }

  def enable(dateTimeMillis: Long): ProvidersType4 = {
    this.copy(startDate = Some(new Timestamp(dateTimeMillis)))
  }

  def enableIfDisabled(dateTimeMillis: Long): ProvidersType4 = {
    if (startDate.isEmpty) {
      this.copy(startDate = Some(new Timestamp(dateTimeMillis)))
    } else {
      this
    }
  }
}