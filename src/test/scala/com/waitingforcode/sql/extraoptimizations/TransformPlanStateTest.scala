package com.waitingforcode.sql.extraoptimizations

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, LocalRelation, Project}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.{FlatSpec, Matchers}

class TransformPlanStateTest extends FlatSpec with Matchers {

  "transformation method" should "move modified plan to the next level" in {
    val letterReference = AttributeReference("letter", StringType, false)()
    val nameReference = AttributeReference("name", StringType, false)()
    val ageReference = AttributeReference("age", IntegerType, false)()
    val selectExpressions = Seq[NamedExpression](
      letterReference, nameReference
    )
    val dataset = LocalRelation(Seq(letterReference, nameReference, ageReference))
    val project1 = Project(selectExpressions, dataset)
    val project2 = Project(selectExpressions, dataset)
    val join = Join(project1, project2, JoinType("fullouter"),
      Option(And(selectExpressions(0), selectExpressions(1))))

    val projectionsFromJoin = new scala.collection.mutable.ListBuffer[Project]()
    // transform is called only once;
    // the goal is to show you that the modified plan is passed to next levels
    join.transformUp {
      case project: Project => {
        project.copy(projectList = project.projectList ++ project.projectList)
      }
      case join: Join => {
        join.children.filter(childPlan => childPlan.isInstanceOf[Project])
          .foreach(childPlan => {
            projectionsFromJoin.append(childPlan.asInstanceOf[Project])
          })
        join
      }
    }

    val expectedFields = Seq("letter", "name", "letter", "name")
    projectionsFromJoin.foreach(projection => {
      projection.projectList.map(namedExpression => namedExpression.name) should contain allElementsOf expectedFields
    })
  }
}
