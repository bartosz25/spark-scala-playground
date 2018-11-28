package com.waitingforcode.graphx.faulttolerance

import com.waitingforcode.graphx.representation.{Friend, RelationshipAttributes}
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph}
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.scalatest.{FlatSpec, Matchers}

class CheckpointTest extends FlatSpec with Matchers {

  private def TestSparkContext(withCheckpoint: Boolean) = {
    val context = SparkContext.getOrCreate(new SparkConf().setAppName("GraphX checkpoint")
      .setMaster("local[*]"))
    if (withCheckpoint) context.setCheckpointDir("/tmp/graphx-checkpoint")
    context
  }

  behavior of "checkpointing"

  it should "fail when the checkpoint directory is not defined" in {
    val checkpointError = intercept[SparkException] {
      graph(false).checkpoint()
    }

    checkpointError.getMessage should include ("Checkpoint directory has not been set in the SparkContext")
  }

  it should "be blocking operation" in {
    val graphWithCheckpointDir = graph(true)

    val graphWithMappedVertices = graphWithCheckpointDir.mapVertices {
      case (vertexId, vertexValue) => vertexValue.copy(name = s"Copy ${vertexValue.name}")
    }

    // Here we want to checkpoint the graph after mapping vertices
    val beforeCheckpoint = System.currentTimeMillis()
    graphWithMappedVertices.checkpoint()
    val afterCheckpoint = System.currentTimeMillis()

    graphWithMappedVertices.collectEdges(EdgeDirection.Either)

    afterCheckpoint > beforeCheckpoint shouldBe true
  }

  it should "restore the graph from checkpoint" in {
    // Please notice that GraphX advises to use Pregel for iterative computation
    // However to not introduce too many concepts at once I'll use it more naive implementation
    val accumulatedVertices = new scala.collection.mutable.HashMap[String, Seq[String]]
    val checkpoints = new scala.collection.mutable.HashSet[String]()
    val graphWithCheckpointDir = graph(true)
    var currentGraph = graphWithCheckpointDir
    for (i <- 1 until 5) {
      val graphWithMappedVertices = currentGraph.mapVertices {
        case (vertexId, vertexValue) => {
          if (i == 5) {
            throw new RuntimeException("Expected failure")
          }
          vertexValue.copy(name = s"Copy ${vertexValue.name}_${i}")
        }
      }

      // Here we want to checkpoint the graph after mapping vertices
      graphWithMappedVertices.checkpoint()
      currentGraph.getCheckpointFiles.foreach(checkpointFile => checkpoints.add(checkpointFile))
      if (i == 5) {
        intercept[SparkException] {
          graphWithMappedVertices.vertices.collectAsMap()
        }
      } else {
        accumulatedVertices.put(s"run1_${i}",
          graphWithMappedVertices.vertices.collect().map {
            case (vertexId, friendVertex) => friendVertex.name
          })
      }
      currentGraph = graphWithMappedVertices
    }
    for (i <- 4 to 10) {
      val graphWithMappedVertices = currentGraph.mapVertices {
        case (vertexId, vertexValue) => vertexValue.copy(name = s"Copy ${vertexValue.name}_${i}")
      }
      graphWithMappedVertices.checkpoint()
      currentGraph.getCheckpointFiles.foreach(checkpointFile => checkpoints.add(checkpointFile))
      accumulatedVertices.put(s"run2_${i}",
        graphWithMappedVertices.vertices.collect().map {
          case (vertexId, friendVertex) => friendVertex.name
        })
      currentGraph = graphWithMappedVertices
    }

    accumulatedVertices should have size 11
    accumulatedVertices.keys should contain allOf("run1_1", "run1_2", "run1_3", "run1_4",
      "run2_4", "run2_5", "run2_6", "run2_7", "run2_8", "run2_9", "run2_10")
    accumulatedVertices("run1_1") should contain allOf("Copy A_1", "Copy B_1", "Copy C_1")
    accumulatedVertices("run1_2") should contain allOf("Copy Copy A_1_2", "Copy Copy B_1_2", "Copy Copy C_1_2")
    accumulatedVertices("run1_3") should contain allOf("Copy Copy Copy A_1_2_3", "Copy Copy Copy B_1_2_3",
      "Copy Copy Copy C_1_2_3")
    accumulatedVertices("run1_4") should contain allOf("Copy Copy Copy Copy A_1_2_3_4", "Copy Copy Copy Copy B_1_2_3_4",
      "Copy Copy Copy Copy C_1_2_3_4")
    accumulatedVertices("run2_4") should contain allOf("Copy Copy Copy Copy Copy A_1_2_3_4_4",
      "Copy Copy Copy Copy Copy B_1_2_3_4_4", "Copy Copy Copy Copy Copy C_1_2_3_4_4")
    accumulatedVertices("run2_5") should contain allOf("Copy Copy Copy Copy Copy Copy A_1_2_3_4_4_5",
      "Copy Copy Copy Copy Copy Copy B_1_2_3_4_4_5", "Copy Copy Copy Copy Copy Copy C_1_2_3_4_4_5")
    accumulatedVertices("run2_6") should contain allOf("Copy Copy Copy Copy Copy Copy Copy A_1_2_3_4_4_5_6",
      "Copy Copy Copy Copy Copy Copy Copy B_1_2_3_4_4_5_6", "Copy Copy Copy Copy Copy Copy Copy C_1_2_3_4_4_5_6")
    accumulatedVertices("run2_7") should contain allOf("Copy Copy Copy Copy Copy Copy Copy Copy A_1_2_3_4_4_5_6_7",
      "Copy Copy Copy Copy Copy Copy Copy Copy B_1_2_3_4_4_5_6_7",
      "Copy Copy Copy Copy Copy Copy Copy Copy C_1_2_3_4_4_5_6_7")
    accumulatedVertices("run2_8") should contain allOf("Copy Copy Copy Copy Copy Copy Copy Copy Copy A_1_2_3_4_4_5_6_7_8",
      "Copy Copy Copy Copy Copy Copy Copy Copy Copy B_1_2_3_4_4_5_6_7_8",
      "Copy Copy Copy Copy Copy Copy Copy Copy Copy C_1_2_3_4_4_5_6_7_8")
    accumulatedVertices("run2_9") should contain allOf("Copy Copy Copy Copy Copy Copy Copy Copy Copy Copy A_1_2_3_4_4_5_6_7_8_9",
      "Copy Copy Copy Copy Copy Copy Copy Copy Copy Copy B_1_2_3_4_4_5_6_7_8_9",
      "Copy Copy Copy Copy Copy Copy Copy Copy Copy Copy C_1_2_3_4_4_5_6_7_8_9")
    accumulatedVertices("run2_10") should contain allOf("Copy Copy Copy Copy Copy Copy Copy Copy Copy Copy Copy A_1_2_3_4_4_5_6_7_8_9_10",
      "Copy Copy Copy Copy Copy Copy Copy Copy Copy Copy Copy B_1_2_3_4_4_5_6_7_8_9_10",
      "Copy Copy Copy Copy Copy Copy Copy Copy Copy Copy Copy C_1_2_3_4_4_5_6_7_8_9_10")
    checkpoints should have size 10
  }


  private def graph(withCheckpoint: Boolean) = {
    val vertices = TestSparkContext(withCheckpoint).parallelize(
      Seq((1L, Friend("A", 20)), (2L, Friend("B", 21)), (3L, Friend("C", 22)))
    )
    val edges = TestSparkContext(withCheckpoint).parallelize(
      Seq(Edge(1L, 2L, RelationshipAttributes(0L, 1)), Edge(1L, 3L, RelationshipAttributes(0L, 0)))
    )

    Graph(vertices, edges)
  }

}
