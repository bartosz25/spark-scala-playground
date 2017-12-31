package com.waitingforcode.sql

import com.waitingforcode.util.InMemoryLogAppender
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class LogicalPlanOptimizersTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder().appName("Logical Plan Optimizer test").master("local")
    .getOrCreate()

  import sparkSession.implicits._
  private val Users = Seq(
    (1, "user_1")
  ).toDF("id", "login")

  private val NewUsers = Seq(
    (2, "user_2"), (3, "user_3")
  ).toDF("id", "login")

  private val UserOrders = Seq(
    (2, "order_1"), (1, "order_2")
  ).toDF("user_id", "order_name")

  private val AllOrdersPerUser = Seq(
    (1, Array(100, 200, 300)), (2, Array(100, 300, 100)), (3, Array(9, 8, 102))
  ).toDF("user_id", "orders_amounts")

  override def afterAll() {
    sparkSession.stop()
  }

  "redundant cast" should "be detected when string column is casted to string type" in {
    val logAppender = createLogAppender()
    val castedUsers = Users.select(Users("login").cast("string"))

    val queryExecutionPlan = castedUsers.queryExecution.toString()

    // In logs we're expecting to find the following entry:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.SimplifyCasts ===
    // !Project [cast(_2#3 as string) AS login#10]   Project [_2#3 AS login#10]
    // +- LocalRelation [_1#2, _2#3]                +- LocalRelation [_1#2, _2#3]
    // (org.apache.spark.sql.internal.BaseSessionStateBuilder$$anon$2:62)
    queryExecutionPlan should include("== Optimized Logical Plan ==\nLocalRelation [login")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.SimplifyCasts ===")
  }

  "nested case conversion" should "be simplified to only 1 conversion" in {
    val logAppender = createLogAppender()
    val upperCasedLogins = Users.select(upper(lower(Users("login"))).as("upper_cased_login"))

    val queryExecutionPlan = upperCasedLogins.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.SimplifyCaseConversionExpressions ===
    // !Project [upper(lower(_2#3)) AS upper_cased_login#20]   Project [upper(_2#3) AS upper_cased_login#20]
    // +- LocalRelation [_1#2, _2#3]                          +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nLocalRelation [upper_cased_login")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.SimplifyCaseConversionExpressions ===")
  }

  "redundant selected columns" should "be removed in optimized plan" in {
    val logAppender = createLogAppender()
    val redundantSelectQuery = Users.select("login").select("login")

    val queryExecutionPlan = redundantSelectQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.CollapseProject ===
    // !Project [login#6]                            Project [_2#3 AS login#6]
    // !+- Project [_1#2 AS id#5, _2#3 AS login#6]   +- LocalRelation [_1#2, _2#3]
    // !   +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nLocalRelation [login")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.CollapseProject ===")
  }

  "two repartition calls" should "be transformed to a single one" in {
    val logAppender = createLogAppender()
    val redundantRepartitionOperations = Users.select("login").repartition(10).repartition(20)

    val queryExecutionPlan = redundantRepartitionOperations.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.CollapseRepartition ===
    // Repartition 20, true                     Repartition 20, true
    // !+- Repartition 10, true                  +- Project [login#6]
    // !   +- Project [login#6]                     +- Project [_2#3 AS login#6]
    // !      +- Project [_2#3 AS login#6]             +- LocalRelation [_1#2, _2#3]
    // !         +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nRepartition 20, true\n+- LocalRelation [login")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.CollapseRepartition ===")
  }

  "two coalesce calls" should "be transformed to a single one" in {
    val logAppender = createLogAppender()
    val redundantRepartitionOperations = Users.select("login").coalesce(10).coalesce(20)

    val queryExecutionPlan = redundantRepartitionOperations.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.CollapseRepartition ===
    //  Repartition 20, false                    Repartition 20, false
    // !+- Repartition 10, false                 +- Project [login#6]
    // !   +- Project [login#6]                     +- Project [_2#3 AS login#6]
    // !      +- Project [_2#3 AS login#6]             +- LocalRelation [_1#2, _2#3]
    // !         +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nRepartition 20, false\n+- LocalRelation [login#")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.CollapseRepartition ===")
  }

  "not used columns" should "be removed from the projection" in {
    val logAppender = createLogAppender()
    val notUsedColumnsQuery = Users.select("login")

    val queryExecutionPlan = notUsedColumnsQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.ColumnPruning ===
    // Project [login#6]                            Project [login#6]
    // !+- Project [_1#2 AS id#5, _2#3 AS login#6]   +- Project [_2#3 AS login#6]
    // +- LocalRelation [_1#2, _2#3]                +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nLocalRelation [login#")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.ColumnPruning ===")
  }

  "trivial filter" should "be removed" in {
    val logAppender = createLogAppender()
    val trivialFiltersQuery = Users.select("id", "login").where("2 > 1")

    val queryExecutionPlan = trivialFiltersQuery.queryExecution.toString()

    // The logs should contain the proof of using PruneFilters optimization rule:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.PruneFilters ===
    //   Project [_1#2 AS id#5, _2#3 AS login#6]   Project [_1#2 AS id#5, _2#3 AS login#6]
    // !+- Filter true                            +- LocalRelation [_1#2, _2#3]
    // !   +- LocalRelation [_1#2, _2#3]
    // (org.apache.spark.sql.internal.BaseSessionStateBuilder$$anon$2:62)
    queryExecutionPlan should include("== Optimized Logical Plan ==\nLocalRelation [id")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.PruneFilters ===")
  }

  "empty sortable expressions" should "be removed from the order by query" in {
    val logAppender = createLogAppender()
    val queryWithNoSortableExpression = Users.sort()

    val queryExecutionPlan = queryWithNoSortableExpression.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.EliminateSorts ===
    // !Sort true                                    Project [_1#2 AS id#5, _2#3 AS login#6]
    // !+- Project [_1#2 AS id#5, _2#3 AS login#6]   +- LocalRelation [_1#2, _2#3]
    // !   +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nLocalRelation [id")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.EliminateSorts ===")
  }

  "no-op columns used in ordering" should "be removed" in {
    val logAppender = createLogAppender()
    val queryWithNoSortableExpression = Users.withColumn("hash_number", lit(19203)).sort(asc("hash_number"))

    val queryExecutionPlan = queryWithNoSortableExpression.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.EliminateSorts ===
    // !Sort [19203 ASC NULLS FIRST], true                                    Project [_1#2 AS id#5, _2#3 AS login#6, 19203 AS hash_number#20]
    // !+- Project [_1#2 AS id#5, _2#3 AS login#6, 19203 AS hash_number#20]   +- LocalRelation [_1#2, _2#3]
    // !   +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nLocalRelation [id")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.EliminateSorts ===")
  }

  "columns" should "be pruned when they are not used in the query" in {
    val logAppender = createLogAppender()
    val queryWithPrunedColumns = Users.select("id", "login").where("id > 0")

    val queryExecutionPlan = queryWithPrunedColumns.queryExecution.toString()

    // The following message should be written in logs:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.ColumnPruning ===
    // !Project [id#5, login#6]                      Project [_1#2 AS id#5, _2#3 AS login#6]
    // !+- Project [_1#2 AS id#5, _2#3 AS login#6]   +- Filter (_1#2 > 0)
    // !   +- Filter (_1#2 > 0)                         +- LocalRelation [_1#2, _2#3]
    // !      +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nProject [_1")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.ColumnPruning ===")
  }

  "3 filters" should "be combined into a single one filter" in {
    val logAppender = createLogAppender()
    val combinedFiltersQuery = Users.select("id", "login")
      .where("id > 0").where("id < 100").where("login != 'blacklisted'")

    val queryExecutionPlan = combinedFiltersQuery.queryExecution.toString()

    // Expected message in the logs:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.CombineFilters ===
    // Project [_1#2 AS id#5, _2#3 AS login#6]                                     Project [_1#2 AS id#5, _2#3 AS login#6]
    // !+- Filter ((_1#2 < 100) && (isnotnull(_2#3) && NOT (_2#3 = blacklisted)))   +- Filter ((_1#2 > 0) && (((_1#2 < 100) && isnotnull(_2#3)) && NOT (_2#3 = blacklisted)))
    // !   +- Filter (_1#2 > 0)                                                        +- LocalRelation [_1#2, _2#3]
    // !      +- LocalRelation [_1#2, _2#3]
    // (org.apache.spark.sql.internal.BaseSessionStateBuilder$$anon$2:62)
    queryExecutionPlan should include("+- Filter ((((_1")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.CombineFilters ===")
  }

  "3 limits" should "be combined into a single one limit with the least value from 3" in {
    val logAppender = createLogAppender()
    val combinedLimitsQuery = Users.select("id", "login").limit(10).limit(2).limit(15)

    val queryExecutionPlan = combinedLimitsQuery.queryExecution.toString()

    // Expected message in the logs:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.CombineLimits ===
    // !GlobalLimit 15                                              GlobalLimit least(2, 15)
    // !+- LocalLimit 15                                            +- LocalLimit least(2, 15)
    // !   +- GlobalLimit 2                                            +- GlobalLimit 10
    // !      +- LocalLimit 2                                             +- LocalLimit 10
    // !         +- GlobalLimit 10                                           +- Project [_1#2 AS id#5, _2#3 AS login#6]
    // !            +- LocalLimit 10                                            +- LocalRelation [_1#2, _2#3]
    // !               +- Project [_1#2 AS id#5, _2#3 AS login#6]
    // !                  +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nGlobalLimit 2\n+- LocalLimit 2")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.CombineLimits ===")
  }

  "filter with LIKE'text%'" should "be simplified with startsWith method" in {
    val logAppender = createLogAppender()
    val likeSimplifiedQuery = Users.select("id", "login").where("login LIKE 'text%'")

    val queryExecutionPlan = likeSimplifiedQuery.queryExecution.toString()

    // Expected message in the logs:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.LikeSimplification ===
    // Project [_1#2 AS id#5, _2#3 AS login#6]          Project [_1#2 AS id#5, _2#3 AS login#6]
    // !+- Filter (isnotnull(_2#3) && _2#3 LIKE text%)   +- Filter (isnotnull(_2#3) && StartsWith(_2#3, text))
    // +- LocalRelation [_1#2, _2#3]                    +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include(" && StartsWith(_2")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.LikeSimplification ===")
  }

  "filter with LIKE'%text%'" should "be simplified with contains method" in {
    val logAppender = createLogAppender()
    val likeSimplifiedQuery = Users.select("id", "login").where("login LIKE '%text%'")

    val queryExecutionPlan = likeSimplifiedQuery.queryExecution.toString()

    // Expected message in logs:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.LikeSimplification ===
    // Project [_1#2 AS id#5, _2#3 AS login#6]           Project [_1#2 AS id#5, _2#3 AS login#6]
    // !+- Filter (isnotnull(_2#3) && _2#3 LIKE %text%)   +- Filter (isnotnull(_2#3) && Contains(_2#3, text))
    // +- LocalRelation [_1#2, _2#3]                     +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include(") && Contains(_2")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.LikeSimplification ===")
  }

  "the same limit" should "be applied to 2 unions" in {
    val logAppender = createLogAppender()
    val unionedAndLimitedUsers = Users.union(NewUsers).limit(1)

    val queryExecutionPlan = unionedAndLimitedUsers.queryExecution.toString()
    // Expected message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.LimitPushDown ===
    // GlobalLimit 1                                          GlobalLimit 1
    // +- LocalLimit 1                                        +- LocalLimit 1
    // +- Union                                               +- Union
    //  !      :- Project [_1#2 AS id#5, _2#3 AS login#6]             :- LocalLimit 1
    // !      :  +- LocalRelation [_1#2, _2#3]                       :  +- Project [_1#2 AS id#5, _2#3 AS login#6]
    // !      +- Project [_1#12 AS id#15, _2#13 AS login#16]         :     +- LocalRelation [_1#2, _2#3]
    // !         +- LocalRelation [_1#12, _2#13]                     +- LocalLimit 1
    // !                                                                +- Project [_1#12 AS id#15, _2#13 AS login#16]
    // !                                                                   +- LocalRelation [_1#12, _2#13]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nGlobalLimit 1" +
      "\n+- LocalLimit 1" +
      "\n   +- Union" +
      "\n      :- LocalLimit 1")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.LimitPushDown ===")
  }

  "global union limit" should "be used instead of local limit" in {
    val logAppender = createLogAppender()
    val unionedAndLimitedUsers = Users.union(NewUsers.limit(3)).limit(1)

    val queryExecutionPlan = unionedAndLimitedUsers.queryExecution.toString()

    // Expected log message
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.LimitPushDown ===
    // GlobalLimit 1                                                GlobalLimit 1
    // +- LocalLimit 1                                              +- LocalLimit 1
    // +- Union                                                     +- Union
    //  !      :- Project [_1#2 AS id#5, _2#3 AS login#6]                   :- LocalLimit 1
    // !      :  +- LocalRelation [_1#2, _2#3]                             :  +- Project [_1#2 AS id#5, _2#3 AS login#6]
    // !      +- GlobalLimit 3                                             :     +- LocalRelation [_1#2, _2#3]
    // !         +- LocalLimit 3                                           +- LocalLimit 1
    // !            +- Project [_1#12 AS id#15, _2#13 AS login#16]            +- LocalLimit 3
    // !               +- LocalRelation [_1#12, _2#13]                           +- Project [_1#12 AS id#15, _2#13 AS login#16]
    // !                                                                            +- LocalRelation [_1#12, _2#13]
    queryExecutionPlan should include("== Optimized Logical Plan ==" +
      "\nGlobalLimit 1" +
      "\n+- LocalLimit 1" +
      "\n   +- Union" +
      "\n      :- LocalLimit 1")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.LimitPushDown ===")
  }

  "not changed union limits" should "be returned after optimization" in {
    val logAppender = createLogAppender()
    val unionedAndLimitedUsers = Users.union(NewUsers.limit(1)).limit(3)

    val queryExecutionPlan = unionedAndLimitedUsers.queryExecution.toString()

    // Expected log message
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.LimitPushDown ===
    //   GlobalLimit 3                                                GlobalLimit 3
    // +- LocalLimit 3                                              +- LocalLimit 3
    // +- Union                                                     +- Union
    //   !      :- Project [_1#2 AS id#5, _2#3 AS login#6]                   :- LocalLimit 3
    // !      :  +- LocalRelation [_1#2, _2#3]                             :  +- Project [_1#2 AS id#5, _2#3 AS login#6]
    // !      +- GlobalLimit 1                                             :     +- LocalRelation [_1#2, _2#3]
    // !         +- LocalLimit 1                                           +- GlobalLimit 1
    // !            +- Project [_1#12 AS id#15, _2#13 AS login#16]            +- LocalLimit 1
    // !               +- LocalRelation [_1#12, _2#13]                           +- Project [_1#12 AS id#15, _2#13 AS login#16]
    // !                                                                            +- LocalRelation [_1#12, _2#13]
    queryExecutionPlan should include("== Optimized Logical Plan ==" +
      "\nGlobalLimit 3" +
      "\n+- LocalLimit 3" +
      "\n   +- Union" +
      "\n      :- LocalLimit 3" +
      "\n      :  +- LocalRelation ")
    queryExecutionPlan should include("+- GlobalLimit 1" +
      "\n         +- LocalLimit 1" +
      "\n            +- LocalRelation [id#")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.LimitPushDown ===")
  }

  "IN filtering clause with duplicated values" should "be optimized" in {
    val logAppender = createLogAppender()
    val inFilteredUsersQuery = Users.where("login IN('a', 'a', 'b', 'c')")

    val queryExecutionPlan = inFilteredUsersQuery.queryExecution.toString()

    // Expected message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.OptimizeIn ===
    // Project [_1#2 AS id#5, _2#3 AS login#6]   Project [_1#2 AS id#5, _2#3 AS login#6]
    // !+- Filter _2#3 uuuuuIN (a,a,b,c)               +- Filter _2#3 IN (a,b,c)
    // +- LocalRelation [_1#2, _2#3]             +- LocalRelation [_1#2, _2#3]
    // (org.apache.spark.sql.internal.BaseSessionStateBuilder$$anon$2:62)
    queryExecutionPlan should include("IN (a,b,c)")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.OptimizeIn ===")
  }

  "foldable expression" should "be propagated in order clause" in {
    val logAppender = createLogAppender()
    val combinedUnionQuery = sparkSession.sql("SELECT NOW() AS current_time ORDER BY current_time")

    val queryExecutionPlan = combinedUnionQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.FoldablePropagation ===
    // !Sort [current_time#20 ASC NULLS FIRST], true       Sort [1505404900993000 ASC NULLS FIRST], true
    // +- Project [1505404900993000 AS current_time#20]   +- Project [1505404900993000 AS current_time#20]
    // +- OneRowRelation$                                 +- OneRowRelation$
    queryExecutionPlan should include("+- Scan OneRowRelation")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.FoldablePropagation ===")
  }

  "nullable expression" should "be transformed to a literal" in {
    val logAppender = createLogAppender()
    val constantsFoldedQuery = sparkSession.sql("SELECT 'a' != null")

    val queryExecutionPlan = constantsFoldedQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.NullPropagation ===
    // !Project [NOT (a = null) AS (NOT (a = CAST(NULL AS STRING)))#20]   Project [null AS (NOT (a = CAST(NULL AS STRING)))#20]
    // +- OneRowRelation$                                                +- OneRowRelation$
    queryExecutionPlan should include("== Optimized Logical Plan ==\nProject [null AS (NOT (a = CAST(NULL AS STRING)))")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.NullPropagation ===")
  }

  "concatenation of 3 letters" should "be transformed to a constant" in {
    val logAppender = createLogAppender()
    val constantsFoldedQuery = sparkSession.sql("SELECT CONCAT('a', CONCAT('a', 'a')) AS tripleA")

    val queryExecutionPlan = constantsFoldedQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===
    // !Project [concat(a, concat(a, a)) AS tripleA#20]   Project [aaa AS tripleA#20]
    // +- OneRowRelation$                                +- OneRowRelation$
    queryExecutionPlan should include("== Optimized Logical Plan ==\nProject [aaa AS tripleA")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===")
  }

  "duplicated map operation" should "be deserialized only once" in {
    val logAppender = createLogAppender()
    val constantsFoldedQuery = Users.select("login").map(_.toString()).map(_.toString())

    val queryExecutionPlan = constantsFoldedQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.EliminateSerialization ===
    // SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true) AS value#30]            SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true) AS value#30]
    //+- MapElements <function1>, class java.lang.String, [StructField(value,StringType,true)], obj#29: java.lang.String                                                        +- MapElements <function1>, class java.lang.String, [StructField(value,StringType,true)], obj#29: java.lang.String
    //  !   +- DeserializeToObject value#25.toString, obj#28: java.lang.String                                                                                                        +- Project [obj#24 AS obj#28]
    //  !      +- SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true) AS value#25]         +- MapElements <function1>, interface org.apache.spark.sql.Row, [StructField(login,StringType,true)], obj#24: java.lang.String
    //    !         +- MapElements <function1>, interface org.apache.spark.sql.Row, [StructField(login,StringType,true)], obj#24: java.lang.String                                            +- DeserializeToObject createexternalrow(login#6.toString, StructField(login,StringType,true)), obj#23: org.apache.spark.sql.Row
    //      !            +- DeserializeToObject createexternalrow(login#6.toString, StructField(login,StringType,true)), obj#23: org.apache.spark.sql.Row                                          +- Project [_2#3 AS login#6]
    //      !               +- Project [_2#3 AS login#6]                                                                                                                                              +- LocalRelation [_1#2, _2#3]
    //      !                  +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nSerializeFromObject [staticinvoke")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.EliminateSerialization ===")
  }

  "duplicated mapping not changing the object" should "be considered as redundant alias" in {
    val logAppender = createLogAppender()
    val redundantAliasesQuery = Users.select("login").map(_.toString).map(_.toString)

    val queryExecutionPlan = redundantAliasesQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.RemoveRedundantAliases ===
    // SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true) AS value#30]   SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true) AS value#30]
    // +- MapElements <function1>, class java.lang.String, [StructField(value,StringType,true)], obj#29: java.lang.String                                               +- MapElements <function1>, class java.lang.String, [StructField(value,StringType,true)], obj#29: java.lang.String
    //   !   +- Project [obj#24 AS obj#28]                                                                                                                                    +- Project [obj#24]
    //   +- MapElements <function1>, interface org.apache.spark.sql.Row, [StructField(login,StringType,true)], obj#24: java.lang.String                                   +- MapElements <function1>, interface org.apache.spark.sql.Row, [StructField(login,StringType,true)], obj#24: java.lang.String
    //     +- DeserializeToObject createexternalrow(login#6.toString, StructField(login,StringType,true)), obj#23: org.apache.spark.sql.Row                                 +- DeserializeToObject createexternalrow(login#6.toString, StructField(login,StringType,true)), obj#23: org.apache.spark.sql.Row
    //     +- Project [_2#3 AS login#6]                                                                                                                                     +- Project [_2#3 AS login#6]
    //     +- LocalRelation [_1#2, _2#3]
    // We can observe that the optimized plan doesn't create an intermediate (redundant) alias: obj#28 in the 3rd
    // operation from the top.                                                                                                                                  +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nSerializeFromObject [staticinvoke")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.RemoveRedundantAliases ===")
  }

  "filters on property value" should "be enriched with null check filter" in {
    val logAppender = createLogAppender()
    val inferredFiltersQuery = Users.join(UserOrders, Users("id") === UserOrders("user_id"), "inner")
      .filter("login != 'user_1'")

    val queryExecutionPlan = inferredFiltersQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints ===
    // Join Inner, (id#5 = user_id#25)                            Join Inner, (id#5 = user_id#25)
    // :- Project [_1#2 AS id#5, _2#3 AS login#6]                 :- Project [_1#2 AS id#5, _2#3 AS login#6]
    // !:  +- Filter NOT (_2#3 = user_1)                           :  +- Filter (isnotnull(_2#3) && NOT (_2#3 = user_1))
    // :     +- LocalRelation [_1#2, _2#3]                        :     +- LocalRelation [_1#2, _2#3]
    // +- Project [_1#22 AS user_id#25, _2#23 AS order_name#26]   +- Project [_1#22 AS user_id#25, _2#23 AS order_name#26]
    // +- LocalRelation [_1#22, _2#23]                            +- LocalRelation [_1#22, _2#23]
    queryExecutionPlan should include(":  +- Filter (isnotnull(_2")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints ===")
  }

  "filters on one side o join" should "be pushed to this side and executed before the join operation" in {
    val logAppender = createLogAppender()
    val pushedPredicateJoinQuery = Users.join(UserOrders, Users("id") === UserOrders("user_id"), "inner")
      .filter("order_name != 'removed'")

    val queryExecutionPlan = pushedPredicateJoinQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.PushPredicateThroughJoin ===
    // !Filter NOT (order_name#26 = removed)                          Join Inner, (id#5 = user_id#25)
    // !+- Join Inner, (id#5 = user_id#25)                            :- Project [_1#2 AS id#5, _2#3 AS login#6]
    // !   :- Project [_1#2 AS id#5, _2#3 AS login#6]                 :  +- LocalRelation [_1#2, _2#3]
    // !   :  +- LocalRelation [_1#2, _2#3]                           +- Filter NOT (order_name#26 = removed)
    // +- Project [_1#22 AS user_id#25, _2#23 AS order_name#26]      +- Project [_1#22 AS user_id#25, _2#23 AS order_name#26]
    // +- LocalRelation [_1#22, _2#23]                               +- LocalRelation [_1#22, _2#23]
    queryExecutionPlan should include("   +- Filter (isnotnull(_2")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.PushPredicateThroughJoin ===")
  }


  "correlated subquery" should "be rewritten in LEFT OUTER JOIN" in {
    val logAppender = createLogAppender()
    Users.createTempView("Users")
    val correlatedSubqueryQuery =
      Users.sqlContext.sql("SELECT login, (SELECT max(id) FROM Users WHERE U.login=login) AS max_id FROM Users U")

    val queryExecutionPlan = correlatedSubqueryQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.RewriteCorrelatedScalarSubquery ===
    // !Project [_2#3 AS login#6, scalar-subquery#31 [(_2#3 = login#6#38)] AS max_id#32]   Project [_2#3 AS login#6, max(id)#34 AS max_id#32]
    // !:  +- Aggregate [login#6], [max(id#5) AS max(id)#34, login#6 AS login#6#38]        +- Project [_1#2, _2#3, max(id)#34]
    // !:     +- LocalRelation [id#5, login#6]                                                +- Join LeftOuter, (_2#3 = login#6#38)
    // !+- LocalRelation [_1#2, _2#3]                                                            :- LocalRelation [_1#2, _2#3]
    // !                                                                                         +- Aggregate [login#6], [max(id#5) AS max(id)#34, login#6 AS login#6#38]
    // !                                                                                            +- LocalRelation [id#5, login#6]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nProject ")
    queryExecutionPlan should include("+- Join LeftOuter, ")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.RewriteCorrelatedScalarSubquery ===")
  }

  "the always false expression" should "be simplified" in {
    val logAppender = createLogAppender()
    Users.createTempView("Users_SimplifyConditionals")
    val simplifiedConditionalsQuery = sparkSession.sql("SELECT CASE WHEN 10 > 20 THEN \"10_20\" ELSE \"20_10\" END " +
      "FROM Users_SimplifyConditionals")

    val queryExecutionPlan = simplifiedConditionalsQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.SimplifyConditionals ===
    // !Project [CASE WHEN false THEN 10_20 ELSE 20_10 END AS CASE WHEN (10 > 20) THEN 10_20 ELSE 20_10 END#41]   Project [20_10 AS CASE WHEN (10 > 20) THEN 10_20 ELSE 20_10 END#41]
    // +- LocalRelation [_1#2, _2#3]                                                                             +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Analyzed Logical Plan ==\nCASE WHEN (10 > 20) THEN 10_20 ELSE 20_10 END: string" +
      "\nProject [CASE WHEN (10 > 20) THEN 10_20 ELSE 20_10 END AS CASE WHEN (10 > 20) THEN 10_20 ELSE 20_10 END")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.SimplifyConditionals ===")
  }

  "not changing if-else expression" should "be simplified to a constant" in {
    val logAppender = createLogAppender()
    Users.createTempView("Users_SimplifyConditionals2")
    val simplifiedConditionalsQuery = sparkSession.sql("SELECT IF(10 > 20, \"A\", \"B\") " +
      "FROM Users_SimplifyConditionals2")

    val queryExecutionPlan = simplifiedConditionalsQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.SimplifyConditionals ===
    // !Project [if (false) A else B AS (IF((10 > 20), A, B))#41]   Project [B AS (IF((10 > 20), A, B))#41]
    // +- LocalRelation [_1#2, _2#3]                               +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nLocalRelation [(IF((10 > 20), A, B))")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.SimplifyConditionals ===")
  }

  "variable if-else expression" should "not be simplified" in {
    val logAppender = createLogAppender()
    Users.createTempView("Users_SimplifyConditionals3")
    val simplifiedConditionalsQuery = sparkSession.sql("SELECT IF(id > 20, \"id>20\", \"id<=20\") " +
      "FROM Users_SimplifyConditionals3")

    val queryExecutionPlan = simplifiedConditionalsQuery.queryExecution.toString()

    queryExecutionPlan should include("== Optimized Logical Plan ==\nLocalRelation [(IF((id > 20), id>20, id<=20))")
    logAppender.getMessagesText().mkString("\n") should not include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.Users_SimplifyConditionals3 ===")
  }

  "array optimizer" should "be used when only 1 entry is used from created array" in {
    val logAppender = createLogAppender()
    AllOrdersPerUser.createTempView("All_Orders_test1")
    val arrayStructOptimizedQuery = AllOrdersPerUser.sqlContext.sql(
      "SELECT array(1*orders_amounts[0], 2*orders_amounts[1])[1] AS the_last_weighted_order FROM All_Orders_test1")

    val queryExecutionPlan = arrayStructOptimizedQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.SimplifyCreateArrayOps ===
    // !Project [array((1 * _2#33[0]), (2 * _2#33[1]))[1] AS the_last_weighted_order#41]   Project [(2 * _2#33[1]) AS the_last_weighted_order#41]
    // +- LocalRelation [_1#32, _2#33]                                                    +- LocalRelation [_1#32, _2#33]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nLocalRelation [the_last_weighted_order")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.SimplifyCreateArrayOps ===")
  }

  "array optimizer" should "be used when only the array field is used from created structure" in {
    val logAppender = createLogAppender()
    AllOrdersPerUser.createTempView("All_Orders_test2")
    val arrayStructOptimizedQuery = AllOrdersPerUser.sqlContext.sql(
      "SELECT array(named_struct('user', user_id), named_struct('user', 1000))[0].user AS first_user_id FROM All_Orders_test2")

    val queryExecutionPlan = arrayStructOptimizedQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.SimplifyCreateArrayOps ===
    // !Project [array(named_struct(user, _1#32), [1000])[0].user AS first_user_id#41]   Project [named_struct(user, _1#32).user AS first_user_id#41]
    // +- LocalRelation [_1#32, _2#33]                                        +- LocalRelation [_1#32, _2#33]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nLocalRelation [first_user_id")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.SimplifyCreateArrayOps ===")
  }

  "structure creation" should "be optimized when only one field is used" in {
    val logAppender = createLogAppender()
    AllOrdersPerUser.createTempView("All_Orders_test3")
    val structOptimizedQuery = AllOrdersPerUser.sqlContext.sql(
      "SELECT named_struct('user', user_id, 'orders', array(1, 2*orders_amounts[1])).orders AS all_orders FROM All_Orders_test3")

    val queryExecutionPlan = structOptimizedQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.SimplifyCreateStructOps ===
    // !Project [named_struct(user, _1#32, orders, array(1, (2 * _2#33[1]))).orders AS all_orders#41]   Project [array(1, (2 * _2#33[1])) AS all_orders#41]
    // +- LocalRelation [_1#32, _2#33]                                                                 +- LocalRelation [_1#32, _2#33]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nLocalRelation [all_orders")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.SimplifyCreateStructOps ===")
  }

  "map optimizer" should "be used when only one key is used from created map" in {
    val logAppender = createLogAppender()
    AllOrdersPerUser.createTempView("All_Orders_test4")
    val mapOptimizedStructQuery = AllOrdersPerUser.sqlContext.sql(
      "SELECT map('user', user_id, 'first_order_amount', orders_amounts[0]).first_order_amount AS first_order_amount " +
        "FROM All_Orders_test4")

    val queryExecutionPlan = mapOptimizedStructQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.SimplifyCreateMapOps ===
    // !Project [map(user, _1#32, first_order_amount, _2#33[0])[first_order_amount] AS first_order_amount#41]   Project [CASE WHEN (first_order_amount = user) THEN _1#32 WHEN (
    //   first_order_amount = first_order_amount) THEN _2#33[0] END AS first_order_amount#41]
    // +- LocalRelation [_1#32, _2#33]                                                                         +- LocalRelation [_1#32, _2#33]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nLocalRelation [first_order_amount")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.SimplifyCreateMapOps ===")
  }

  "static binary expression on id equality" should "be converted to appropriate literal" in {
    val logAppender = createLogAppender()
    val simplifiedBinaryExpressionQuery = Users.select(Users("id") === Users("id"))

    val queryExecutionPlan = simplifiedBinaryExpressionQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.SimplifyBinaryComparison ===
    // !Project [(_1#2 <=> _1#2) AS (id <=> id)#30]   Project [true AS (id <=> id)#30]
    // +- LocalRelation [_1#2, _2#3]                 +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Analyzed Logical Plan ==\n(id = id): boolean\nProject ")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.SimplifyBinaryComparison ===")
  }

  "static binary expression on id" should "be converted to true literal" in {
    val logAppender = createLogAppender()
    val simplifiedBinaryExpressionQuery = Users.select(Users("id") <=> Users("id"))

    val queryExecutionPlan = simplifiedBinaryExpressionQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.SimplifyBinaryComparison ===
    // !Project [(_1#2 <=> _1#2) AS (id <=> id)#30]   Project [true AS (id <=> id)#30]
    // +- LocalRelation [_1#2, _2#3]                 +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Analyzed Logical Plan ==\n(id <=> id): boolean\nProject ")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.SimplifyBinaryComparison ===")
  }

  "redundant boolean expressions" should "be simplified" in {
    val logAppender = createLogAppender()
    val simplifiedBoolExpressionQuery = Users.select("id", "login").where("(id > 0 OR login == 'test') AND id > 0")

    val queryExecutionPlan = simplifiedBoolExpressionQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.BooleanSimplification ===
    // Project [_1#2 AS id#5, _2#3 AS login#6]                   Project [_1#2 AS id#5, _2#3 AS login#6]
    // !+- Filter (((_1#2 > 0) || (_2#3 = test)) && (_1#2 > 0))   +- Filter (_1#2 > 0)
    // +- LocalRelation [_1#2, _2#3]                             +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("+- Filter (_1")
    queryExecutionPlan should include("> 0)")
    queryExecutionPlan should not include(" == 'test'")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.BooleanSimplification ===")
  }

  "redundant boolean expressions with negation" should "be simplified" in {
    val logAppender = createLogAppender()
    val simplifiedBoolExpressionQuery = Users.select("id", "login").where("(NOT(id > 0) OR login == 'test') AND id > 0")

    val queryExecutionPlan = simplifiedBoolExpressionQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.BooleanSimplification ===
    //  Project [_1#2 AS id#5, _2#3 AS login#6]                       Project [_1#2 AS id#5, _2#3 AS login#6]
    // !+- Filter ((NOT (_1#2 > 0) || (_2#3 = test)) && (_1#2 > 0))   +- Filter ((_2#3 = test) && (_1#2 > 0))
    // +- LocalRelation [_1#2, _2#3]                                 +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include(" = test)) && (_1")
    queryExecutionPlan should include("> 0))")
    queryExecutionPlan should not include("!_")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.BooleanSimplification ===")
  }

  "redundant positive node" should "be removed from the optimized plan" in {
    val logAppender = createLogAppender()
    Users.createTempView("Users_test_positive")
    val queryWithoutDispensableExpressions = Users.sqlContext.sql("SELECT positive(id) FROM Users_test_positive")

    val queryExecutionPlan = queryWithoutDispensableExpressions.queryExecution.toString()
    println(s"${queryExecutionPlan }")

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.RemoveDispensableExpressions ===
    // !Project [positive(_1#2) AS (+ id)#41]   Project [_1#2 AS (+ id)#41]
    // +- LocalRelation [_1#2, _2#3]           +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nLocalRelation [(+ id)")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.RemoveDispensableExpressions ===")
  }

  "additions" should "be reordered to a constant" in {
    val logAppender = createLogAppender()
    Users.createTempView("Users_ReorderedAssocOps")
    val queryReorderedAssociativeOperations = sparkSession.sql("select id+1+3+5+7+9 FROM Users_ReorderedAssocOps")

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.ReorderAssociativeOperator ===
    // !Project [(((((_1#2 + 1) + 3) + 5) + 7) + 9) AS (((((id + 1) + 3) + 5) + 7) + 9)#42]   Project [(_1#2 + 25) AS (((((id + 1) + 3) + 5) + 7) + 9)#42]
    // +- LocalRelation [_1#2, _2#3]                                                         +- LocalRelation [_1#2, _2#3]
    val queryExecutionPlan = queryReorderedAssociativeOperations.queryExecution.toString()
    queryExecutionPlan should include("== Optimized Logical Plan ==\nLocalRelation [(((((id + 1) + 3) + 5) + 7) + 9)")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.ReorderAssociativeOperator ===")
  }

  "full outer join" should "be replaced by inner join when a filter is applied on both sides of join" in {
    val logAppender = createLogAppender()
    val eliminatedOuterJoinQuery = Users.join(UserOrders, Users("id") === UserOrders("user_id"), "outer")
      .where("login != 'user_1' AND order_name != 'o'")

    val queryExecutionPlan = eliminatedOuterJoinQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.EliminateOuterJoin ===
    // Filter (NOT (login#6 = user_1) && NOT (order_name#26 = o))    Filter (NOT (login#6 = user_1) && NOT (order_name#26 = o))
    // !+- Join FullOuter, (id#5 = user_id#25)                        +- Join Inner, (id#5 = user_id#25)
    // :- Project [_1#2 AS id#5, _2#3 AS login#6]                    :- Project [_1#2 AS id#5, _2#3 AS login#6]
    // :  +- LocalRelation [_1#2, _2#3]                              :  +- LocalRelation [_1#2, _2#3]
    // +- Project [_1#22 AS user_id#25, _2#23 AS order_name#26]      +- Project [_1#22 AS user_id#25, _2#23 AS order_name#26]
    // +- LocalRelation [_1#22, _2#23]                               +- LocalRelation [_1#22, _2#23]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nJoin Inner,")
    queryExecutionPlan should include(":  +- Filter (isnotnull(_2")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.EliminateOuterJoin ===")
  }

  "full outer join" should "be replaced by right outer join when the right side has predicate" in {
    val logAppender = createLogAppender()
    val eliminatedOuterJoinQuery = Users.join(UserOrders, Users("id") === UserOrders("user_id"), "outer")
      .where("order_name != 'o'")

    val queryExecutionPlan = eliminatedOuterJoinQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.EliminateOuterJoin ===
    // Filter NOT (order_name#26 = o)                                Filter NOT (order_name#26 = o)
    // !+- Join FullOuter, (id#5 = user_id#25)                        +- Join RightOuter, (id#5 = user_id#25)
    // :- Project [_1#2 AS id#5, _2#3 AS login#6]                    :- Project [_1#2 AS id#5, _2#3 AS login#6]
    // :  +- LocalRelation [_1#2, _2#3]                              :  +- LocalRelation [_1#2, _2#3]
    // +- Project [_1#22 AS user_id#25, _2#23 AS order_name#26]      +- Project [_1#22 AS user_id#25, _2#23 AS order_name#26]
    // +- LocalRelation [_1#22, _2#23]                               +- LocalRelation [_1#22, _2#23]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nJoin RightOuter, (id")
    queryExecutionPlan should include("+- Filter (isnotnull(_2")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.EliminateOuterJoin ===")
  }

  "right outer join" should "be replaced inner join when both sides have predicate" in {
    val logAppender = createLogAppender()
    val eliminatedOuterJoinQuery = Users.join(UserOrders, Users("id") === UserOrders("user_id"), "rightouter")
      .where("id > 0 AND order_name != 'o'")

    val queryExecutionPlan = eliminatedOuterJoinQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.EliminateOuterJoin ===
    // Filter ((id#5 > 0) && NOT (order_name#26 = o))                Filter ((id#5 > 0) && NOT (order_name#26 = o))
    // !+- Join RightOuter, (id#5 = user_id#25)                       +- Join Inner, (id#5 = user_id#25)
    // :- Project [_1#2 AS id#5, _2#3 AS login#6]                    :- Project [_1#2 AS id#5, _2#3 AS login#6]
    // :  +- LocalRelation [_1#2, _2#3]                              :  +- LocalRelation [_1#2, _2#3]
    // +- Project [_1#22 AS user_id#25, _2#23 AS order_name#26]      +- Project [_1#22 AS user_id#25, _2#23 AS order_name#26]
    //+- LocalRelation [_1#22, _2#23]                               +- LocalRelation [_1#22, _2#23]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nJoin Inner, (id")
    queryExecutionPlan should include(":  +- Filter (_1")
    queryExecutionPlan should include("   +- Filter ((isnotnull(_2")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.EliminateOuterJoin ===")
  }

  "plain select query on 3 tables" should "be changed to 2 joins with ON clauses corresponding to WHERE clauses from the initial query" in {
    val logAppender = createLogAppender()
    Users.createTempView("Users_ReorderJoin1")
    NewUsers.createTempView("Users_ReorderJoin2")
    NewUsers.createTempView("Users_ReorderJoin3")
    val reorderedJoinQuery = sparkSession.sql("SELECT u1.id FROM Users_ReorderJoin1 u1, Users_ReorderJoin2 u2, Users_ReorderJoin3 u3 " +
      "WHERE u1.id = u2.id AND u2.login = u3.login")

    val queryExecutionPlan = reorderedJoinQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.ReorderJoin ===
    // Project [id#5]                                            Project [id#5]
    // !+- Filter ((id#5 = id#15) && (login#16 = login#44))       +- Join Inner, (login#16 = login#44)
    // !   +- Join Inner                                             :- Join Inner, (id#5 = id#15)
    // !      :- Join Inner                                          :  :- Project [_1#2 AS id#5, _2#3 AS login#6]
    // !      :  :- Project [_1#2 AS id#5, _2#3 AS login#6]          :  :  +- LocalRelation [_1#2, _2#3]
    // !      :  :  +- LocalRelation [_1#2, _2#3]                    :  +- Project [_1#12 AS id#15, _2#13 AS login#16]
    // !      :  +- Project [_1#12 AS id#15, _2#13 AS login#16]      :     +- LocalRelation [_1#12, _2#13]
    // !      :     +- LocalRelation [_1#12, _2#13]                  +- Project [_1#12 AS id#43, _2#13 AS login#44]
    // !      +- Project [_1#12 AS id#43, _2#13 AS login#44]            +- LocalRelation [_1#12, _2#13]
    // !         +- LocalRelation [_1#12, _2#13]
    // As you can see above, the optimized plan executes filtering directly as "JOIN ON..." condition.
    queryExecutionPlan should include("== Optimized Logical Plan ==\nProject")
    queryExecutionPlan should include("+- Join Inner, (login")
    queryExecutionPlan should include("  :  +- Join Inner, (id")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.ReorderJoin ===")
  }

  "joins without defined join column" should "be reordered to inner joins" in {
    val logAppender = createLogAppender()
    val reorderedJoinQuery =
      Users.as("u1").join(NewUsers.as("u2")).join(Users.as("u3")).where("u1.id = u2.id AND u1.login = u3.login")

    val queryExecutionPlan = reorderedJoinQuery.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.ReorderJoin ===
    // !Filter ((id#5 = id#15) && (login#6 = login#49))        Join Inner, (login#6 = login#49)
    // !+- Join Inner                                          :- Join Inner, (id#5 = id#15)
    // !   :- Join Inner                                       :  :- Project [_1#2 AS id#5, _2#3 AS login#6]
    // !   :  :- Project [_1#2 AS id#5, _2#3 AS login#6]       :  :  +- LocalRelation [_1#2, _2#3]
    // !   :  :  +- LocalRelation [_1#2, _2#3]                 :  +- Project [_1#12 AS id#15, _2#13 AS login#16]
    // !   :  +- Project [_1#12 AS id#15, _2#13 AS login#16]   :     +- LocalRelation [_1#12, _2#13]
    // !   :     +- LocalRelation [_1#12, _2#13]               +- Project [_1#2 AS id#48, _2#3 AS login#49]
    // !   +- Project [_1#2 AS id#48, _2#3 AS login#49]           +- LocalRelation [_1#2, _2#3]
    // !      +- LocalRelation [_1#2, _2#3]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nJoin Inner, (login#")
    queryExecutionPlan should include(":- Join Inner, (id")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.ReorderJoin ===")
  }

  "redundant projection" should "be removed from the optimized plan" in {
    val logAppender = createLogAppender()
    val queryWithRemovedRedundantProjection = Users.join(UserOrders, Users("id") === UserOrders("user_id"))
      .select("id", "login", "user_id", "order_name")
      .where("id = 0")
      .select("id", "login", "user_id", "order_name")

    val queryExecutionPlan = queryWithRemovedRedundantProjection.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.RemoveRedundantProject ===
    // !Project [id#5, login#6, user_id#25, order_name#26]               Filter ((user_id#25 = 0) && (id#5 = 0))
    // !+- Filter ((user_id#25 = 0) && (id#5 = 0))                       +- Join Inner, (id#5 = user_id#25)
    // !   +- Join Inner, (id#5 = user_id#25)                               :- Project [_1#2 AS id#5, _2#3 AS login#6]
    // !      :- Project [_1#2 AS id#5, _2#3 AS login#6]                    :  +- LocalRelation [_1#2, _2#3]
    // !      :  +- LocalRelation [_1#2, _2#3]                              +- Project [_1#22 AS user_id#25, _2#23 AS order_name#26]
    // !      +- Project [_1#22 AS user_id#25, _2#23 AS order_name#26]         +- LocalRelation [_1#22, _2#23]
    // !         +- LocalRelation [_1#22, _2#23]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nJoin Inner,")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.RemoveRedundantProject ===")
  }

  "projection" should "be pushed down to both tables from UNION" in {
    val logAppender = createLogAppender()
    val queryWithPushedProjectionToUnions = Users.union(NewUsers).select("id", "login")

    val queryExecutionPlan = queryWithPushedProjectionToUnions.queryExecution.toString()

    // Expected log message:
    // === Applying Rule org.apache.spark.sql.catalyst.optimizer.PushProjectionThroughUnion ===
    // !Project [id#5, login#6]                             Union
    // !+- Union                                            :- Project [id#5, login#6]
    // !   :- Project [_1#2 AS id#5, _2#3 AS login#6]       :  +- Project [_1#2 AS id#5, _2#3 AS login#6]
    // !   :  +- LocalRelation [_1#2, _2#3]                 :     +- LocalRelation [_1#2, _2#3]
    // !   +- Project [_1#12 AS id#15, _2#13 AS login#16]   +- Project [id#15, login#16]
    // !      +- LocalRelation [_1#12, _2#13]                  +- Project [_1#12 AS id#15, _2#13 AS login#16]
    // !                                                          +- LocalRelation [_1#12, _2#13]
    queryExecutionPlan should include("== Optimized Logical Plan ==\nUnion\n:- LocalRelation [id")
    logAppender.getMessagesText().mkString("\n") should include("=== Applying Rule " +
      "org.apache.spark.sql.catalyst.optimizer.PushProjectionThroughUnion ===")
  }

  private def createLogAppender(): InMemoryLogAppender = {
    InMemoryLogAppender.createLogAppender(Seq("Applying Rule"))
  }
}
