package com.waitingforcode.sql.extraoptimizations

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class TransformTest extends FlatSpec with Matchers with BeforeAndAfter {

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


  "transformDown" should "only apply to the current node and children" in {
    val transformedPlans = new scala.collection.mutable.ListBuffer[String]()
    selectStatement.transformDown {
      case lp => {
        transformedPlans.append(lp.nodeName)
        lp
      }
    }

    transformedPlans should have size 2
    transformedPlans should contain inOrder("Project", "LocalRelation")
  }

  "transformUp" should "apply to the children and current node" in {
    val transformedPlans = new scala.collection.mutable.ListBuffer[String]()
    selectStatement.transformUp {
      case lp => {
        transformedPlans.append(lp.nodeName)
        lp
      }
    }

    transformedPlans should have size 2
    transformedPlans should contain inOrder("LocalRelation", "Project")
  }

}
