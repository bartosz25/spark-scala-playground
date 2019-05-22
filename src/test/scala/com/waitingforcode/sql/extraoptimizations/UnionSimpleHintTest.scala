package com.waitingforcode.sql.extraoptimizations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, Coalesce, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class UnionSimpleHintTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder().appName("Union Hint - simple version test")
    .master("local[*]")
    .withExtensions(extensions => {
      extensions.injectResolutionRule(_ => UnionSimpleTransformer)
    })
    .getOrCreate()

  "UNION rewritter" should "transform a UNION of 2 Datasets into JOIN" in {
    import sparkSession.implicits._
    val dataset1 = Seq(("A", 1, 1), ("B", 2, 1), ("C", 3, 1), ("D", 4, 1), ("E", 5, 1)).toDF("letter", "nr", "a_flag")
    val dataset2 = Seq(("A", 1, 1), ("E", 5, 1), ("F", 10, 1), ("G", 11, 1), ("H", 12, 1)).toDF("letter", "nr", "a_flag")

    dataset1.createOrReplaceTempView("dataset_1")
    dataset2.createOrReplaceTempView("dataset_2")
    val rewrittenQuery = sparkSession.sql(
      """SELECT letter, nr, a_flag FROM dataset_1
        |UNION SELECT letter, nr, a_flag FROM dataset_2""".stripMargin)
    rewrittenQuery.explain(true)

    val unionData = rewrittenQuery.map(row => s"${row.getAs[String]("letter")}-${row.getAs[Int]("nr")}-${row.getAs[Int]("a_flag")}")
      .collect()
    unionData should have size 8
    unionData should contain allOf("A-1-1", "B-2-1",  "C-3-1", "D-4-1", "E-5-1", "F-10-1", "G-11-1", "H-12-1")
    rewrittenQuery.queryExecution.executedPlan.toString should include("SortMergeJoin")
  }

}

object UnionSimpleTransformer extends Rule[LogicalPlan] {

  override def apply(logicalPlan: LogicalPlan): LogicalPlan = logicalPlan transform {
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
        // TODO: try to replace with pattern matching/partial function
        if (remainingExpressions.isEmpty) {
          expression
        } else {
          concatExpressions(And(expression, remainingExpressions.head), remainingExpressions.tail)
        }
      }
      val concatenatedExpressions = concatExpressions(equalToExpressions.head, equalToExpressions.tail)

      val projection1 = Project(joinColumns(0), union.children(0))
      val projection2 = Project(joinColumns(1), union.children(1))
      val join = Join(projection1, projection2, JoinType("fullouter"), Option(concatenatedExpressions))
      combineProjection(joinPairs, join)
    }
  }

  private def combineProjection(joinAttributes: Seq[Seq[Attribute]], childPlan: Join): Project = {
    val fields = joinAttributes.map(attributes => {
      Alias(Coalesce(attributes), attributes(0).name)()
    })
    Project(fields, childPlan)
  }


}
