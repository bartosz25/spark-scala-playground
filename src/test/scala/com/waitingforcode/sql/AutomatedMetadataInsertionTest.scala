package com.waitingforcode.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.scalatest.{FlatSpec, Matchers}

class AutomatedMetadataInsertionTest extends FlatSpec with Matchers {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL automatic metadata insertion")
    .master("local[*]").getOrCreate()
  import sparkSession.implicits._

  "metadata" should "be automatically inserted after each step" in {
    val InputOrders = Seq(
      (1L, 1L, 2018), (2L, 1L, 2019), (3L, 2L, 2019), (4L, 3L, 2018), (5L, 3L, 2019), (6L, 3L, 2019)
    ).toDF("order_id", "customer_id", "year")

    val filteredAccumulator = new LongAccumulator()
    val allOrdersAccumulator = new LongAccumulator()
    sparkSession.sparkContext.register(filteredAccumulator)
    sparkSession.sparkContext.register(allOrdersAccumulator)

    val filteredOrders = InputOrders.filter(order => {
      val is2019Order = order.getAs[Int]("year") == 2019
      if (!is2019Order) filteredAccumulator.add(1L)
      allOrdersAccumulator.add(1L)
      is2019Order
    })

    import org.apache.spark.sql.functions.udf
    val resolveCustomerName = udf(EnrichmentService.get _)
    val mappedOrders = filteredOrders.withColumn("customer_name", resolveCustomerName($"customer_id"))

    val notEnrichedCustomersAccumulator = new LongAccumulator()
    val enrichedCustomersAccumulator = new LongAccumulator()
    sparkSession.sparkContext.register(notEnrichedCustomersAccumulator)
    sparkSession.sparkContext.register(enrichedCustomersAccumulator)
    val ordersPerCustomer = mappedOrders.groupByKey(order => order.getAs[Long]("customer_id"))
      .mapGroups {
        case (customerId, orders)=> {
          var counter = 0L
          orders.foreach(order => {
            // We suppose the same customer_name value for all rows
            if (counter == 0) {
              if (order.getAs[String]("customer_name") == null) {
                notEnrichedCustomersAccumulator.add(1L)
              } else {
                enrichedCustomersAccumulator.add(1L)
              }
            }
            counter += 1
          })
          (customerId, counter)
        }
      }.select($"_1".alias("customer_id"), $"_2".alias("orders_number"))

    ordersPerCustomer.show()

    filteredAccumulator.value shouldEqual 2
    allOrdersAccumulator.value shouldEqual 6
    notEnrichedCustomersAccumulator.value shouldEqual 2
    enrichedCustomersAccumulator.value shouldEqual 1
  }

}

object EnrichmentService {
  private val EnrichmentData = Map(3L -> "User 3")

  def get(userId: Long) = EnrichmentData.get(userId)
}