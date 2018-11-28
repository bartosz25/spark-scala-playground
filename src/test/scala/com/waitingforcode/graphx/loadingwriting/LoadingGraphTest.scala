package com.waitingforcode.graphx.loadingwriting

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class LoadingGraphTest extends FlatSpec with Matchers with BeforeAndAfter {

  private val EdgesFile = new File("/tmp/graphx-loading/edges.txt")
  private val EdgesDatasetFile = new File("/tmp/graphx-loading/edges-dataset.txt")
  private val VerticesDatasetFile = new File("/tmp/graphx-loading/vertices-dataset.txt")

  before {
    val edges =
      """
        |1  3
        |1 2
        |2  5""".stripMargin
    FileUtils.writeStringToFile(EdgesFile, edges)
  }

  after {
    EdgesFile.delete()
    EdgesDatasetFile.delete()
    VerticesDatasetFile.delete()
  }

  "GraphLoader" should "load graph from a file" in {
    val testSparkContext = SparkContext.getOrCreate(new SparkConf().setAppName("GraphX graph loading")
      .setMaster("local[*]"))
    val graph = GraphLoader.edgeListFile(testSparkContext, EdgesFile.getPath)

    val vertices = graph.vertices.collect().map {
      case (vertexId, attrs) => s"(${vertexId})[${attrs}]"
    }
    val edges = graph.edges.collect().map(edge => s"(${edge.srcId})--[${edge.attr}]-->(${edge.dstId})")

    vertices should have size 4
    vertices should contain allOf("(2)[1]", "(1)[1]", "(3)[1]", "(5)[1]")
    edges should have size 3
    edges should contain allOf("(1)--[1]-->(2)", "(1)--[1]-->(3)", "(2)--[1]-->(5)")
  }

  "Dataset" should "be used to create a Graph from JSON" in {
    val sparkSession: SparkSession = SparkSession.builder().appName("Graph from JSON").master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._
    val edgesDataset = Seq(
      (1, 2, "A"), (1, 3, "B"), (3, 4, "A")
    ).toDF("sourceId", "destinationId", "attributes")
    val verticesDataset = Seq(
      (1, "vertex1"), (2, "vertex2"), (3, "vertex3")
    ).toDF("vertexId", "attribute")

    edgesDataset.write.mode(SaveMode.Overwrite).json(EdgesDatasetFile.getAbsolutePath)
    verticesDataset.write.mode(SaveMode.Overwrite).json(VerticesDatasetFile.getAbsolutePath)

    val edgesFromDataset = sparkSession.read.json(EdgesDatasetFile.getAbsolutePath).mapPartitions(edgesRows => {
      edgesRows.map(edgeRow => {
        Edge(edgeRow.getAs[Long]("sourceId"), edgeRow.getAs[Long]("destinationId"), edgeRow.getAs[String]("attributes"))
      })
    }).rdd
    val verticesFromDataset = sparkSession.read.json(VerticesDatasetFile.getAbsolutePath).mapPartitions(vertices => {
      vertices.map(vertexRow => (vertexRow.getAs[Long]("vertexId"), vertexRow.getAs[String]("attribute")))
    }).rdd

    val graphFromEdges = Graph(verticesFromDataset, edgesFromDataset, "?")

    val vertices = graphFromEdges.vertices.collect().map {
      case (vertexId, attrs) => s"(${vertexId})[${attrs}]"
    }
    vertices should have size 4
    vertices should contain allOf("(1)[vertex1]", "(2)[vertex2]", "(3)[vertex3]", "(4)[?]")
    val edges = graphFromEdges.edges.collect().map(edge => s"(${edge.srcId})--[${edge.attr}]-->(${edge.dstId})")
    edges should have size 3
    edges should contain allOf("(1)--[A]-->(2)", "(3)--[A]-->(4)", "(1)--[B]-->(3)")
    sparkSession.close()
  }

}
