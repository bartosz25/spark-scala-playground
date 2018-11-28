package com.waitingforcode.sql

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.unsafe.types.CalendarInterval
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class SortMergeJoinTest extends FlatSpec with BeforeAndAfter with Matchers {

  val sparkSession = SparkSession.builder().appName("Sort-merge join test")
    .master("local[*]")
    .config("spark.sql.join.preferSortMergeJoin", "true")
    .config("spark.sql.autoBroadcastJoinThreshold", "1")
    .config("spark.sql.defaultSizeInBytes", "100000")
    .getOrCreate()

  "sort-merge join" should "be used when neither broadcast nor hash join are possible" in {
    import sparkSession.implicits._

    val customersDataFrame = (1 to 3).map(nr => (nr, s"Customer_${nr}")).toDF("cid", "login")
    val ordersDataFrame = Seq(
      (1, 1, 19.5d), (2, 1, 200d), (3, 2, 500d), (4, 100, 1000d),
      (5, 1, 19.5d), (6, 1, 200d), (7, 2, 500d), (8, 100, 1000d)
    ).toDF("id", "customers_id", "amount")

    val ordersWithCustomers = ordersDataFrame.join(customersDataFrame, $"customers_id" === $"cid")
    val mergedOrdersWithCustomers = ordersWithCustomers.collect().map(toAssertRow(_))
    val explainedPlan = ordersWithCustomers.queryExecution.toString()

    explainedPlan.contains("SortMergeJoin [customers_id") shouldBe true
    mergedOrdersWithCustomers.size shouldEqual(6)
    mergedOrdersWithCustomers should contain allOf(
      "1-1-19.5-1-Customer_1", "2-1-200.0-1-Customer_1", "3-2-500.0-2-Customer_2",
      "5-1-19.5-1-Customer_1", "6-1-200.0-1-Customer_1", "7-2-500.0-2-Customer_2"
      )
  }

  "for not sortable keys the sort merge join" should "not be used" in {
    import sparkSession.implicits._
    // Here we explicitly define the schema. Thanks to that we can show
    // the case when sort-merge join won't be used, i.e. when the key is not sortable
    // (there are other cases - when broadcast or shuffle joins can be chosen over sort-merge
    //  but it's not shown here).
    // Globally, a "sortable" data type is:
    // - NullType, one of AtomicType
    // - StructType having all fields sortable
    // - ArrayType typed to sortable field
    // - User Defined DataType backed by a sortable field
    // The method checking sortability is org.apache.spark.sql.catalyst.expressions.RowOrdering.isOrderable
    // As  you see, CalendarIntervalType is not included in any of above points,
    // so even if the data structure is the same (id + login for customers, id + customer id + amount for orders)
    // with exactly the same number of rows, the sort-merge join won't be applied here.
    val schema = StructType(
      Seq(StructField("cid", CalendarIntervalType), StructField("login", StringType))
    )
    val schemaOrder = StructType(
      Seq(StructField("id", IntegerType), StructField("customers_id", CalendarIntervalType), StructField("amount", DoubleType))
    )

    val customersRdd = sparkSession.sparkContext.parallelize((1 to 3).map(nr => (new CalendarInterval(nr, 1000), s"Customer_${nr}")))
      .map(attributes => Row(attributes._1, attributes._2))
    val customersDataFrame = sparkSession.createDataFrame(customersRdd, schema)

    val ordersRdd = sparkSession.sparkContext.parallelize(Seq(
      (1, new CalendarInterval(1, 1000), 19.5d), (2, new CalendarInterval(1, 1000), 200d),
      (3, new CalendarInterval(2, 1000), 500d), (4, new CalendarInterval(11, 1000), 1000d),
      (5, new CalendarInterval(1, 1000), 19.5d), (6, new CalendarInterval(1, 1000), 200d),
      (7, new CalendarInterval(2, 1000), 500d), (8, new CalendarInterval(11, 1000), 1000d)
    ).map(attributes => Row(attributes._1, attributes._2, attributes._3)))
    val ordersDataFrame = sparkSession.createDataFrame(ordersRdd, schemaOrder)

    val ordersWithCustomers = ordersDataFrame.join(customersDataFrame, $"customers_id" === $"cid")
    val mergedOrdersWithCustomers = ordersWithCustomers.collect().map(toAssertRowInterval(_))
    val explainedPlan = ordersWithCustomers.queryExecution.toString()

    explainedPlan.contains("ShuffledHashJoin [customers_id") shouldBe true
    explainedPlan.contains("SortMergeJoin [customers_id") shouldBe false
    mergedOrdersWithCustomers.size shouldEqual(6)
    mergedOrdersWithCustomers should contain allOf(
      "1-1:1-19.5-1:1-Customer_1", "2-1:1-200.0-1:1-Customer_1", "5-1:1-19.5-1:1-Customer_1",
      "6-1:1-200.0-1:1-Customer_1", "3-2:1-500.0-2:1-Customer_2", "7-2:1-500.0-2:1-Customer_2"
    )
  }

  private def toAssertRowInterval(row: Row): String = {
    val orderId = row.getInt(0)
    val orderCustomerId = row.getAs[CalendarInterval](1)
    val orderAmount = row.getDouble(2)
    val customerId = row.getAs[CalendarInterval](3)
    val customerLogin = row.getString(4)
    s"${orderId}-${orderCustomerId.months}:${orderCustomerId.milliseconds()}-"+
      s"${orderAmount}-${customerId.months}:${customerId.milliseconds()}-${customerLogin}"
  }

  private def toAssertRow(row: Row): String = {
    val orderId = row.getInt(0)
    val orderCustomerId = row.getInt(1)
    val orderAmount = row.getDouble(2)
    val customerId = row.getInt(3)
    val customerLogin = row.getString(4)
    s"${orderId}-${orderCustomerId}-${orderAmount}-${customerId}-${customerLogin}"
  }

}