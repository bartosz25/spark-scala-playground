package com.waitingforcode.sql.extraoptimizations

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class PhysicalOptimizationTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder().appName("Union as join - physical optimization")
    .master("local[*]")
    .withExtensions(extensions => {
      extensions.injectResolutionRule(_ => UnionToJoinLogicalPlanRule)
      extensions.injectPlannerStrategy(_ => UnionToJoinRewrittenStrategy)
    })
    .getOrCreate()


  "UNION" should "be executed as RDD's fullOuterJoin method" in {
    import sparkSession.implicits._
    val dataset1 = Seq(("A", 1, 1), ("B", 2, 1), ("C", 3, 1), ("D", 4, 1), ("E", 5, 1)).toDF("letter", "nr", "a_flag")
    val dataset2 = Seq(("A", 1, 1), ("E", 5, 1), ("F", 10, 1), ("G", 11, 1), ("H", 12, 1)).toDF("letter", "nr", "a_flag")

    dataset1.createOrReplaceTempView("dataset_1")
    dataset2.createOrReplaceTempView("dataset_2")
    val rewrittenQuery = sparkSession.sql(
      """SELECT letter, nr, a_flag FROM dataset_1
        |UNION SELECT letter, nr, a_flag FROM dataset_2""".stripMargin)

    val unionRows = rewrittenQuery.collect().map(r => s"${r.getAs[String]("letter")}-${r.getAs[Int]("nr")}")
    unionRows should have size 8
    unionRows should contain allOf("A-1", "B-2", "C-3", "D-4", "E-5", "F-10", "G-11", "H-12")
    rewrittenQuery.queryExecution.executedPlan.toString should include("UnionJoinExecutor")
  }

}

object UnionToJoinLogicalPlanRule extends Rule[LogicalPlan] {

  override def apply(logicalPlan: LogicalPlan): LogicalPlan = logicalPlan transform {
    case distinct: Distinct if distinct.child.isInstanceOf[Union] => {
      val union = distinct.child.asInstanceOf[Union]
      UnionToJoinRewritten(union.children(0), union.children(1))
    }
  }

}


case class UnionToJoinRewritten(left: LogicalPlan, right: LogicalPlan) extends LogicalPlan {
  override def output: Seq[Attribute] = left.output ++ right.output
  override def children: Seq[LogicalPlan] = Seq(left, right)
}

object UnionToJoinRewrittenStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case union: UnionToJoinRewritten => {
      new UnionJoinExecutorExec(planLater(union.children(0)), planLater(union.children(1))) :: Nil
    }
    case _ => Nil
  }
}


case class UnionJoinExecutorExec(left: SparkPlan, right: SparkPlan) extends SparkPlan { //with CodegenSupport {

  override protected def doExecute(): RDD[InternalRow] = {
    val leftRdd = left.execute().groupBy(row => {
      row.hashCode()
    })
    val rightRdd = right.execute().groupBy(row => {
      row.hashCode()
    })

    leftRdd.fullOuterJoin(rightRdd).mapPartitions( matchedRows => {
      val rowWriter = new UnsafeRowWriter(6)
      matchedRows.map {
        case (_, (leftRows, rightRows)) => {
          rowWriter.reset()
          rowWriter.zeroOutNullBytes()

          val matchedRow = leftRows.getOrElse(rightRows.get).toSeq.head
          val (letter, nr, flag) = if (matchedRow.isNullAt(0)) {
            (matchedRow.getUTF8String(3), matchedRow.getInt(4), matchedRow.getInt(5))
          } else {
            (matchedRow.getUTF8String(0), matchedRow.getInt(1), matchedRow.getInt(2))
          }
          rowWriter.write(0, letter)
          rowWriter.write(1, nr)
          rowWriter.write(2, flag)
          rowWriter.write(3, letter)
          rowWriter.write(4, nr)
          rowWriter.write(5, flag)
          rowWriter.getRow
        }
      }
    })
  }

  override def output: Seq[Attribute] =  left.output  ++ right.output //

  override def children: Seq[SparkPlan] = Seq(left, right)

}