package com.waitingforcode.graphx.edge

import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

class EdgeRddTest extends FlatSpec with Matchers {

  private val TestSparkContext = SparkContext.getOrCreate(new SparkConf().setAppName("EdgeRDD tests")
    .setMaster("local[*]"))

  behavior of "EdgeRDD"

  it should "filter and map edges" in {
    val edges = TestSparkContext.parallelize(Seq(Edge(1L, 2L, "A"), Edge(1L, 3L, "B"), Edge(2L, 4L, "C")))

    val graph = Graph.fromEdges(edges, "...")

    val edgesNotStartingFromVertex1 = graph.edges.filter(edge => edge.srcId != 1)
      .mapPartitions(edges => edges.map(edge => s"${edge.srcId}=${edge.attr}"))

    val collectedEdges = edgesNotStartingFromVertex1.collect()
    collectedEdges should have size 1
    collectedEdges(0) shouldEqual "2=C"
  }

  it should "reverse the edges direction" in {
    val edges = TestSparkContext.parallelize(Seq(Edge(1L, 2L, "A"), Edge(1L, 3L, "B"), Edge(2L, 4L, "C")))
    val graph = Graph.fromEdges(edges, "...")

    val reversedEdges = graph.edges.reverse

    val collectedEdges = reversedEdges.map(edge => s"${edge.srcId}-->${edge.dstId}").collect()
    collectedEdges should have size 3
    collectedEdges should contain allOf("2-->1", "3-->1", "4-->2")
  }

  it should "join 2 edge datasets" in {
    val sharedEdges = Seq(Edge(1L, 2L, "A"), Edge(1L, 3L, "B"), Edge(2L, 4L, "C"))
    val edgesGraph1 = TestSparkContext.parallelize(sharedEdges ++ Seq(Edge(2L, 4L, "Cbis")))
    val graph1 = Graph.fromEdges(edgesGraph1, "...").partitionBy(PartitionStrategy.EdgePartition1D)
    val edgesGraph2 = TestSparkContext.parallelize(Seq(Edge(2L, 4L, "E")) ++ sharedEdges ++
      Seq(Edge(1L, 5L, "D"), Edge(2L, 4L, "F")))
    val graph2 = Graph.fromEdges(edgesGraph2, "...").partitionBy(PartitionStrategy.EdgePartition1D)

    val joinedEdges = graph1.edges.innerJoin(graph2.edges) {
      case (srcVertexId, dstVertexId, graph1Attr, graph2Attr) => s"${graph1Attr}${graph2Attr}"
    }

    val collectedEdges = joinedEdges.collect()
    collectedEdges should have size 4
    // As you can see, the join takes the first declared edge in the underlying arrays.
    // It's why we retrieve all occurrences of (2, 4) from graph1 and only the firt one from the graph2
    collectedEdges should contain allOf(Edge(1L, 2L, "AA"), Edge(1L, 3L, "BB"), Edge(2L, 4L, "CE"), Edge(2,4, "CbisE"))
  }

  it should "not join differently partitioned edges" in {
    val sharedEdges = Seq(Edge(1L, 2L, "A"), Edge(1L, 3L, "B"), Edge(2L, 4L, "C"))
    val edgesGraph1 = TestSparkContext.parallelize(sharedEdges)
    val graph1 = Graph.fromEdges(edgesGraph1, "...").partitionBy(PartitionStrategy.EdgePartition1D)
    val edgesGraph2 = TestSparkContext.parallelize(sharedEdges)
    val graph2 = Graph.fromEdges(edgesGraph2, "...").partitionBy(PartitionStrategy.EdgePartition2D)

    val joinedEdges = graph1.edges.innerJoin(graph2.edges) {
      case (srcVertexId, dstVertexId, graph1Attr, graph2Attr) => s"${graph1Attr}${graph2Attr}"
    }

    val collectedEdges = joinedEdges.collect()
    collectedEdges shouldBe empty
  }

}
