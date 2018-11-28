package com.waitingforcode.graphx.vertex

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}


class VertexRddTest extends FlatSpec with Matchers {

  private val TestSparkContext = SparkContext.getOrCreate(new SparkConf().setAppName("VertexRDD tests")
    .setMaster("local[*]"))

  behavior of "VertexRDD"

  it should "map and filter the vertices" in {
    val vertices = TestSparkContext.parallelize(
      Seq((1L, "A"), (2L, "B"), (3L, "C"))
    )
    val edges = TestSparkContext.parallelize(
      Seq(Edge(1L, 2L, ""))
    )
    val graph = Graph(vertices, edges)

    val mappedAndFilteredVertices = graph.vertices.map(idWithLetter => s"${idWithLetter._2}${idWithLetter._2}")
      .filter(letter => letter != "CC")

    val collectedAttributes = mappedAndFilteredVertices.collect()
    collectedAttributes should have size 2
    collectedAttributes should contain allOf("AA", "BB")
  }

  it should "join 2 vertices datasets and merge their attributes" in {
    val sharedVertices = Seq((1L, "A"), (2L, "B"), (3L, "C"))
    val vertices = TestSparkContext.parallelize(sharedVertices)
    val edges = TestSparkContext.parallelize(Seq(Edge(1L, 2L, "")))
    val graph1 = Graph(vertices, edges)
    val vertices2 = TestSparkContext.parallelize(sharedVertices ++ Seq((4L, "D")))
    val graph2 = Graph(vertices2, edges)

    val joinedVertices = graph1.vertices.innerJoin(graph2.vertices) {
      case (vertexId, vertex1Attr, vertex2Attr) => s"${vertex1Attr}${vertex2Attr}"
    }

    val collectedVertices = joinedVertices.collect()
    collectedVertices should have size 3
    collectedVertices should contain allOf((1L, "AA"), (2L, "BB"), (3L, "CC"))
  }

  it should "get vertices with different values in the second dataset" in {
    val sharedVertices = Seq((1L, "A"), (2L, "B"), (3L, "C"))
    val vertices = TestSparkContext.parallelize(sharedVertices ++ Seq((4L, "D"), (5L, "E")))
    val edges = TestSparkContext.parallelize(Seq(Edge(1L, 2L, "")))
    val graph1 = Graph(vertices, edges)
    val vertices2 = TestSparkContext.parallelize(sharedVertices ++ Seq((4L, "DD"), (5L, "EE"), (6L, "FF")))
    val graph2 = Graph(vertices2, edges)

    val joinedVertices = graph1.vertices.diff(graph2.vertices)

    val collectedVertices = joinedVertices.collect()
    collectedVertices should have size 2
    // As you can see, it doesn't return the vertices of the graph2 that are not in the graph1
    collectedVertices should contain allOf((4L, "DD"), (5L, "EE"))
  }

}
