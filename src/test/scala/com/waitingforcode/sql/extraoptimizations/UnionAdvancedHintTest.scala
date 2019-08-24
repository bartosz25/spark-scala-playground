package com.waitingforcode.sql.extraoptimizations

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, Coalesce, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class UnionAdvancedHintTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder().appName("Advanced UNION hint test").master("local[*]")
    .withExtensions(extensions => {
      // I should use here an optimizer rule but I should then apply it on an HashAggregate + Union which is
      // less understandable than Union + Distinct
      extensions.injectResolutionRule(_ => UnionAdvancedOptimizer)
    })
    .getOrCreate()


  override def afterAll() {
    sparkSession.stop()
  }

  "multiple UNION" should "be rewritten to JOINs" in {
    import sparkSession.implicits._
    val dataset1 = Seq(("A", 1, 1), ("B", 2, 1), ("C", 3, 1), ("D", 4, 1), ("E", 5, 1)).toDF("letter", "nr", "a_flag")
    val dataset2 = Seq(("F", 10, 1), ("G", 11, 1), ("H", 12, 1)).toDF("letter", "nr", "a_flag")
    val dataset3 = Seq(("A", 20, 1), ("B", 21, 1), ("C", 22, 1)).toDF("letter", "nr", "a_flag")
    val dataset4 = Seq(("A", 20, 1), ("B", 21, 1), ("C", 22, 1)).toDF("letter", "nr", "a_flag")

    dataset1.createOrReplaceTempView("dataset_1")
    dataset2.createOrReplaceTempView("dataset_2")
    dataset3.createOrReplaceTempView("dataset_3")
    dataset4.createOrReplaceTempView("dataset_4")
    val rewrittenQuery = sparkSession.sql("SELECT letter, nr, a_flag FROM dataset_1 " +
      "UNION SELECT letter, nr, a_flag FROM dataset_2 " +
      "UNION SELECT letter, nr, a_flag FROM dataset_3 UNION SELECT letter, nr, a_flag FROM dataset_4")

    val unionDataset =
      rewrittenQuery.map(row => s"${row.getAs[String]("letter")}-${row.getAs[Int]("nr")}-${row.getAs[Int]("a_flag")}").collect()

    unionDataset should have size 11
    unionDataset should contain allOf("A-1-1", "A-20-1", "B-2-1", "B-21-1", "C-3-1", "C-22-1", "D-4-1",
      "E-5-1", "F-10-1", "G-11-1", "H-12-1")
    rewrittenQuery.queryExecution.executedPlan.toString should include("SortMergeJoin")
  }

}

object UnionAdvancedOptimizer extends Rule[LogicalPlan] {

  override def apply(logicalPlan: LogicalPlan): LogicalPlan = logicalPlan transformUp {
    case distinct: Distinct if distinct.child.isInstanceOf[Union] => {
      val union = distinct.child.asInstanceOf[Union]
      val joinColumns = union.children.map {
        case localRelation: LocalRelation => localRelation.output
        case project: Project => project.output
      }
      val joinPairs = joinColumns.transpose
      val equalToExpressions = joinPairs.map(attributes => {
        EqualTo(attributes(0), attributes(1))
      })

      def concatExpressions(expression: Expression, remainingExpressions: Seq[Expression]): Expression = {
        if (remainingExpressions.isEmpty) {
          expression
        } else {
          concatExpressions(And(expression, remainingExpressions.head), remainingExpressions.tail)
        }
      }
      val concatenatedExpressions = concatExpressions(equalToExpressions.head, equalToExpressions.tail)

      val projection1 = Project(joinColumns(0), union.children(0))
      val projection2 = Project(joinColumns(1), union.children(1))
      val join1 = Join(projection1, projection2, JoinType("fullouter"), Option(concatenatedExpressions))
      val projection = combineProjection(joinPairs, join1)
      projection
    }
  case other => {
    println(s"Chck oth=${other}")
    other
  }
  }

  private def combineProjection(joinAttributes: Seq[Seq[Attribute]], childPlan: Join): Project = {
    val fields = joinAttributes.map(attributes => {
      Alias(Coalesce(attributes), attributes(0).name)()
    })
    Project(fields, childPlan)
  }

}