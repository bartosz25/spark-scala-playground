package com.waitingforcode.sql.extraoptimizations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.debug
import org.scalatest.{FlatSpec, Matchers}

class NeedCopyResultTest extends FlatSpec with Matchers {

  "the result of sort-merge join" should "be copied" in {
    val sparkSession = SparkSession.builder().appName("Sort-merge join test")
      .master("local[*]")
      .config("spark.sql.join.preferSortMergeJoin", "true")
      .config("spark.sql.autoBroadcastJoinThreshold", "1")
      .config("spark.sql.defaultSizeInBytes", "100000")
      .getOrCreate()

    import sparkSession.implicits._

    val customersDataFrame = (1 to 3).map(nr => (nr, s"Customer_${nr}")).toDF("cid", "login")
    val ordersDataFrame = Seq(
      (1, 1, 19.5d), (2, 1, 200d), (3, 2, 500d), (4, 100, 1000d),
      (5, 1, 19.5d), (6, 1, 200d), (7, 2, 500d), (8, 100, 1000d)
    ).toDF("id", "customers_id", "amount")

    val ordersWithCustomers = ordersDataFrame.join(customersDataFrame, $"customers_id" === $"cid")

    val generatedCode = debug.codegenString(ordersWithCustomers.queryExecution.executedPlan)
    print(generatedCode)
    generatedCode should include("append((smj_mutableStateArray_0[0].getRow()).copy());")
  }

}
