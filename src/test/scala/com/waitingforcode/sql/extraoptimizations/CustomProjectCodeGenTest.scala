package com.waitingforcode.sql.extraoptimizations

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, BoundReference}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, SparkStrategy}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CustomProjectCodeGenTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  "projection" should "be overwriten with a custom physical optimization" in {
    val sparkSession: SparkSession = SparkSession.builder().appName("Custom project rewritten")
      .master("local[1]") // limit parallelism to see counter from generated code in action
      .withExtensions(extensions => {
      extensions.injectResolutionRule(_ => CustomProjectTransformer)
      extensions.injectPlannerStrategy(_ => CustomProjectStrategy)
    })
      .getOrCreate()
    import sparkSession.implicits._
    val dataset = Seq(("A", 1, 1), ("B", 2, 1), ("C", 3, 1), ("D", 4, 1), ("E", 5, 1)).toDF("letter", "nr", "a_flag")

    val allRows = dataset.select("letter", "nr", "a_flag").collect()

    allRows should have size 3
    allRows.map(row => row.mkString(", ")) should contain allOf("A, 1, 1", "C, 3, 1", "E, 5, 1")
  }
}

object CustomProjectTransformer extends Rule[LogicalPlan] {

  override def apply(logicalPlan: LogicalPlan): LogicalPlan = logicalPlan transformDown   {
    case project: Project => {
      CustomProject(project, project.child)
    }
  }
}

case class CustomProject(project: Project, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = {
    val aliasesToMap = project.projectList.map(ne => ne.name)
    child.output.zipWithIndex.map {
      case (expression, index) => Alias(expression, aliasesToMap(index))().toAttribute
    }
  }
}

object CustomProjectStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case project: CustomProject => {
      new CustomProjectExec(project.output, planLater(project.child)) :: Nil
    }
    case _ => Nil
  }
}


case class CustomProjectExec(outputAttrs: Seq[Attribute], child: SparkPlan) extends SparkPlan with CodegenSupport {

  override protected def doExecute(): RDD[InternalRow] = {
    children.head.execute()
  }

  override def output: Seq[Attribute] =  outputAttrs

  override def children: Seq[SparkPlan] = Seq(child)

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    children.map(c => c.execute())
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    val input = ctx.addMutableState("scala.collection.Iterator", "inputsVariable", v => s"$v = inputs[0];")

    val row1 = ctx.freshName("processedRow")
    ctx.INPUT_ROW = row1
    ctx.currentVars = null

    val rowsCounter = ctx.addMutableState(CodeGenerator.JAVA_INT, "count")
    // Generates output variables
    // I take output attributes with indexes and generate the references for them
    // If you want to see what brings the use of consume, you can overwrite this part with
    // ...ref }.take(2)
    // It will throw an exception since the `outputVars` are different from the `def output: Seq[Attribute]`: assertion failed
    //java.lang.AssertionError: assertion failed
    // at scala.Predef$.assert(Predef.scala:156)
    // at org.apache.spark.sql.execution.CodegenSupport$class.consume(WholeStageCodegenExec.scala:147)
    // On the other side, it won't generate an error if you use this.append(ctx.INPUT_ROW) instead but the data you'll
    // collect will be invalid:
    // +------+------------+------+
    //|letter|          nr|a_flag|
    //+------+------------+------+
    //|     A|            |      |
    //|     C|            |      |
    //|     E|            |      |
    //|     A|            |      |
    //|     F|           |      |
    //|     H|           |      |
    //+------+------------+------+
    val outputVars = output.zipWithIndex.map { case (a, i) =>
      val ref = BoundReference(i, a.dataType, a.nullable).genCode(ctx)
      ref
    }

    // using fresh name helps to avoid naming conflicts
    val debugFunctionName = ctx.freshName("printProcessedRow")
    val debugFunction = ctx.addNewFunction(debugFunctionName, s"""
      protected void $debugFunctionName(InternalRow row) {
        System.out.println("Processing " + row);
      }
    """, inlineToOuterClass = true)

    s"""
       |while ($input.hasNext()) {
       |  InternalRow ${ctx.INPUT_ROW} = (InternalRow) $input.next();
       |  ${debugFunction}(${ctx.INPUT_ROW});
       |  if ($rowsCounter % 2 == 0) {
       |    ${consume(ctx, outputVars, ctx.INPUT_ROW)}
       |  } else {
       |    System.out.println("Skipping row because of counter " + $rowsCounter);
       |  }
       |  $rowsCounter = $rowsCounter + 1;
       |  if (shouldStop()) return;
       |}
    """.stripMargin
  }
}
