package com.waitingforcode.graphx.faulttolerance

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class RecomputeTest extends FlatSpec with Matchers with BeforeAndAfter {

  // Since local mode doesn't accept retry, we must try on standalone installation
  private val TestSparkContext = SparkContext.getOrCreate(new SparkConf().setAppName("GraphX automatic recovery")
    .set("spark.task.maxFailures", "5")
    .set("spark.executor.extraClassPath", sys.props("java.class.path"))
    .setMaster("spark://localhost:7077"))

  private val EdgesFile = new File("/tmp/graphx-recompute/edges.txt")

  before {
    val edgesContent =
      """
        |# comment
        |1 2
        |1 3
        |1 4
        |4 2""".stripMargin
    FileUtils.writeStringToFile(EdgesFile, edgesContent)
  }

  after {
    EdgesFile.delete()
  }

  behavior of "recompute recovery"

  it should "recover from temporary failure" in {
    val failingGraph = GraphLoader.edgeListFile(TestSparkContext, EdgesFile.getAbsolutePath)

    val mappedVertices = failingGraph.mapVertices {
      case (vertexId, vertexValue) => {
        if (vertexId == 2 && !FailingFlag.wasFailed) {
          FailingFlag.wasFailed = true
          throw new RuntimeException("Expected failure")
        }
        vertexValue + 10
      }
    }

    val vertices = mappedVertices.vertices.collect().map {
      case (vertexId, vertex) => vertex
    }

    vertices should have size 4
    vertices should contain allElementsOf(Seq(11, 11, 11, 11))
  }

}

object FailingFlag {
  var wasFailed = false
}