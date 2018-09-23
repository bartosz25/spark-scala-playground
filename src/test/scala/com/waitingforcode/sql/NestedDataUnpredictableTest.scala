package com.waitingforcode.sql

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class NestedDataUnpredictableTest extends FlatSpec with Matchers with BeforeAndAfter {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL unstructured nested data test").master("local[*]").getOrCreate()
  import sparkSession.implicits._

  after {
    sparkSession.stop()
  }

  private val OneLevelFile = "/tmp/spark/1_level_unpredictable.json"

  before {
    val oneLevelEntries =
      """
        |{"key": "event_1", "user_context": {"browser": "Firefox", "lang": "en-US"}}
        |{"key": "event_2", "user_context": {"browser": "Firefox"}}
        |{"key": "event_3", "user_context": {"browser": "Google Chrome"}}
        |{"key": "event_4", "geo": {"city": "London", "loc": {"lan": 0, "lat": 1}}}
        |{"key": "event_5", "user_context": {"browser": "Firefox", "lang": "en-US"}}
        |{"key": "event_6", "user_context": {"browser": "Opera", "lang": "en-US"}}
        |{"key": "event_7", "user_context": {"browser": "Firefox", "lang": "en-US"}}
      """.stripMargin
    FileUtils.writeStringToFile(new File(OneLevelFile), oneLevelEntries)
  }

  "DataFrame" should "read 1-level nested data" in {
    val unstructuredDataSchema = StructType(Seq(
      StructField("key", StringType), StructField("user_context", StructType(
        Seq(StructField("browser", StringType), StructField("lang", StringType))
      )), StructField("geo", StructType(
        Seq(StructField("city", StringType), StructField("loc", StructType(
          Seq(StructField("lat", IntegerType), StructField("lon", IntegerType))
        )))
      ))
    ))
    // Since data schema is not consistent, it's better to explicit the schema rather than letting Apache Spark to
    // guess it from sampled lines. It's especially true if the sampling doesn't take all data (and it doesn't for
    // performance reasons !) and thus, it can miss some fields
    val oneLevelsDataFrame = sparkSession.read.schema(unstructuredDataSchema).json(OneLevelFile)

    val eventWithUserBrowserAndCity = oneLevelsDataFrame.select($"key",
      $"user_context.browser".as("user_browser"), $"geo.city", $"geo.loc.lat")

    val collectedUsers = eventWithUserBrowserAndCity.collect()
      .map(row => (row.getAs[String]("key"), row.getAs[String]("user_browser"), row.getAs[String]("city"), row.getAs[Int]("lat")))

    // As you can see, and it's pretty expected, the returned rows have holes for missing values in the data source
    // You can also see that dealing with unstructured data, even in Spark's structured abstraction which is SQL module,
    // is possible. The requirement is however to define the schema explicitly in order to avoid bad surprises at
    // runtime (e.g. schema discovered only in half).
    // The same rule applies on nested fields with more than 1 depth. The example of that was given with `geo.loc` column
    // However if writing the schema manually is tedious and you need to keep source schema,
    // you can always consider to use low level API with RDD abstraction. If you'd use a manual schema with 30 fields
    // belonging to 1 kind of data and 30 other fields to another kind of data, you'll end up with a DataFrame having
    // 60 fields, where 50% of them will be empty. Often it's not acceptable to do so, par example in the application
    // responsible for validating message format and dispatching it into another sink (e.g. Kafka topic with valid messages)
    collectedUsers should have size 7
    collectedUsers should contain allOf(("event_1", "Firefox", null, null), ("event_2", "Firefox", null, null),
      ("event_3", "Google Chrome", null, null), ("event_4", null, "London", 1), ("event_5", "Firefox", null, null),
      ("event_6", "Opera", null, null), ("event_7", "Firefox", null, null))
  }
}
