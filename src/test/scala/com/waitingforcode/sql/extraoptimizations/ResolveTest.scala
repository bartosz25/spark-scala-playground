package com.waitingforcode.sql.extraoptimizations

import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}


class ResolveTest extends FlatSpec with Matchers with BeforeAndAfter {

  private val selectStatement = {
    val letterReference = AttributeReference("letter", StringType, false)()
    val nameReference = AttributeReference("name", StringType, false)()
    val ageReference = AttributeReference("age", IntegerType, false)()
    val selectExpressions = Seq[NamedExpression](
      letterReference, nameReference
    )

    val dataset = LocalRelation(Seq(letterReference, nameReference, ageReference))
    Project(selectExpressions, dataset)
  }

  "resolveOperatorsUp" should "should only apply to the not analyzed nodes" in {
    val resolvedPlans = new scala.collection.mutable.ListBuffer[String]()
    val resolvedPlan = selectStatement.resolveOperatorsUp {
      case project @ Project(selectList, child) => {
        resolvedPlans.append(project.nodeName)
        project
      }
    }
    // Mark the [[Project]] as already analyzed
    SimpleAnalyzer.checkAnalysis(resolvedPlan)

    // Check once again whether the [[Project]] or its children will be resolved once again
    resolvedPlan.resolveOperatorsUp {
        case project @ Project(selectList, child) => {
          resolvedPlans.append(project.nodeName)
          project
        }
        case lp => {
          resolvedPlans.append(lp.nodeName)
          lp
        }
      }

    resolvedPlans should have size 1
    resolvedPlans(0) shouldEqual ("Project")
  }

  "transformDown" should "should apply to analyzed and not analyzed nodes" in {
    val resolvedPlans = new scala.collection.mutable.ListBuffer[String]()
    val resolvedPlan = selectStatement.transformDown {
      case project @ Project(selectList, child) => {
        resolvedPlans.append(project.nodeName)
        project
      }
    }
    SimpleAnalyzer.checkAnalysis(resolvedPlan)

    resolvedPlan.transformDown {
      case project @ Project(selectList, child) => {
        resolvedPlans.append(project.nodeName)
        project
      }
    }

    resolvedPlans should have size 2
    resolvedPlans(0) shouldEqual ("Project")
    resolvedPlans(1) shouldEqual ("Project")
  }
}
