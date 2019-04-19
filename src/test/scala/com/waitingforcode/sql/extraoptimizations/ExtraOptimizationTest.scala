package com.waitingforcode.sql.extraoptimizations


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Add, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.scalatest.{FlatSpec, Matchers}

class ExtraOptimizationTest extends FlatSpec with Matchers {

  "extra optimization rule" should "be added through extraOptimizations field" in {
    val testSparkSession: SparkSession = SparkSession.builder().appName("Extra optimization rules")
      .master("local[*]").getOrCreate()
    import testSparkSession.implicits._
    testSparkSession.experimental.extraOptimizations = Seq(Replace0With3Optimizer)
    Seq(-1, -2, -3).toDF("nr").write.mode("overwrite").json("./test_nrs")
    val optimizedResult = testSparkSession.read.json("./test_nrs").selectExpr("nr + 0")

    optimizedResult.queryExecution.optimizedPlan.toString() should include("Project [(nr#12L + 3) AS (nr + 0)#14L]")
    optimizedResult.collect().map(row => row.getAs[Long]("(nr + 0)")) should contain allOf(0, 1, 2)
  }

  "extra optimization rule" should "be added through extensions" in {
    val testSparkSession: SparkSession = SparkSession.builder().appName("Extra optimization rules")
      .master("local[*]")
      .withExtensions(extensions => {
        extensions.injectOptimizerRule(session => Replace0With3Optimizer)
      })
      .getOrCreate()
    import testSparkSession.implicits._
    testSparkSession.experimental.extraOptimizations = Seq()
    Seq(-1, -2, -3).toDF("nr").write.mode("overwrite").json("./test_nrs")
    val optimizedResult = testSparkSession.read.json("./test_nrs").selectExpr("nr + 0")

    optimizedResult.queryExecution.optimizedPlan.toString() should include("Project [(nr#12L + 3) AS (nr + 0)#14L]")
    optimizedResult.collect().map(row => row.getAs[Long]("(nr + 0)")) should contain allOf(0, 1, 2)
  }

}

object Replace0With3Optimizer extends Rule[LogicalPlan] {

  def apply(logicalPlan: LogicalPlan): LogicalPlan = {
    logicalPlan.transformAllExpressions {
      case Add(left, right) => {
        if (isStaticAdd(left)) {
          right
        } else if (isStaticAdd(right)) {
          Add(left, Literal(3L))
        } else {
          Add(left, right)
        }
      }
    }
  }

  private def isStaticAdd(expression: Expression): Boolean = {
    expression.isInstanceOf[Literal] && expression.asInstanceOf[Literal].toString == "0"
  }

}