package com.waitingforcode.sql.extraoptimizations

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class ResolutionRulesTest extends FlatSpec with Matchers with BeforeAndAfter {

  val sparkSession = SparkSession.builder().appName("Resolution rules test")
    .master("local[*]")
    .withExtensions(extensions => {
      extensions.injectCheckRule(_ => TmpWriteChecker)
      extensions.injectPostHocResolutionRule(_ => ToRootTmpDirectoryResolutionRule)
    })
    .getOrCreate()


  "destination directory" should "be overwritten and the write should fail" in {
    import sparkSession.implicits._
    val ordersDataFrame = Seq(
      (1, 1, 19.5d), (2, 1, 200d), (3, 2, 500d), (4, 100, 1000d),
      (5, 1, 19.5d), (6, 1, 200d), (7, 2, 500d), (8, 100, 1000d)
    ).toDF("id", "customers_id", "amount")

    val error = intercept[AssertionError] {
      ordersDataFrame.select("id").write.mode(SaveMode.Overwrite).json("/home/bartosz/tmp/test-rule")
    }
    error.getMessage shouldEqual "assertion failed: Data can't be written into /tmp/"
  }

}

object TmpWriteChecker extends (LogicalPlan => Unit) {

  def apply(logicalPlan: LogicalPlan): Unit = {
    logicalPlan.foreach {
      case InsertIntoHadoopFsRelationCommand(outputPath, _, _, _, _, _, _, _, _, _, _, _) => {
        println(s"Checking ${outputPath.toString}")
        assert(!outputPath.toString.startsWith("file:/tmp/"), "Data can't be written into /tmp/")
      }
      case _ => {}
    }
  }

}

object ToRootTmpDirectoryResolutionRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case insertDsCommand @ InsertIntoHadoopFsRelationCommand(outputPath, _, _, _, _, _, _, _, _, _, _, _) => {
      if (!outputPath.toString.startsWith("file:///tmp/") && outputPath.toString.contains("/tmp/")) {
        val Array(_, targetDir) = outputPath.toString.split("/tmp/")
        val newTargetDir = s"file:///tmp/${targetDir}"
        println(s"Applying ToRootTmpDirectoryResolutionRule - writing data to ${newTargetDir}")
        insertDsCommand.copy(outputPath = new Path(newTargetDir))
      } else {
        insertDsCommand
      }
    }
  }
}