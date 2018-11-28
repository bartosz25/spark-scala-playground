package com.waitingforcode.graphx.representation

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}


class GraphRepresentationTest extends FlatSpec with Matchers with BeforeAndAfter {

  before {
    InstancesStore.hashCodes.clear()
  }

  private val TestSparkContext = SparkContext.getOrCreate(new SparkConf().setAppName("GraphX graph representation")
    .setMaster("local[*]"))

  behavior of "Graph"

  it should "be a directed property graph" in {
    val incomingEdges = graph.collectEdges(EdgeDirection.In).collect().map {
      case (vertexId, edges) => stringifyEdges(edges)
    }

    incomingEdges should have size 2
    incomingEdges should contain allOf("(1)-[creationTime: 0, maturityLevel: 1]->(2)",
      "(1)-[creationTime: 0, maturityLevel: 0]->(3)")
  }

  it should "reverse edges direction" in {
    val incomingEdges = graph.reverse.collectEdges(EdgeDirection.In).collect().map {
      case (vertexId, edges) => stringifyEdges(edges)
    }

    incomingEdges should have size 1
    incomingEdges(0) shouldEqual "(2)-[creationTime: 0, maturityLevel: 1]->(1) / (3)-[creationTime: 0, maturityLevel: 0]->(1)"
  }

  it should "reverse the graph but keep underlying VertexRDD unchanged" in {
    val sourceGraph = graph
    sourceGraph.vertices.foreach {
      case (vertexId, vertex) => InstancesStore.addHashCode(vertex.hashCode())
    }
    val reversedGraph = sourceGraph.reverse
    reversedGraph.vertices.foreach {
      case (vertexId, vertex) => InstancesStore.addHashCode(vertex.hashCode())
    }

    sourceGraph should not equal reversedGraph
    // Even if both RDDs are not the same, they both store the same objects - you can
    // see that in hashCodes set that has always 3 elements
    sourceGraph.vertices should not equal reversedGraph.vertices
    InstancesStore.hashCodes should have size 3
  }

  it should "map the vertices and change the underlying VertexRDD" in {
    val sourceGraph = graph
    sourceGraph.vertices.foreach {
      case (vertexId, vertex) => InstancesStore.addHashCode(vertex.hashCode())
    }
    val mappedGraph = sourceGraph.mapVertices {
      case (vertexId, vertex) => (vertexId, vertex)
    }
    mappedGraph.vertices.foreach {
      case (vertexId, vertex) => InstancesStore.addHashCode(vertex.hashCode())
    }

    sourceGraph should not equal mappedGraph
    sourceGraph.vertices should not equal mappedGraph.vertices
    InstancesStore.hashCodes should have size 6
  }

  it should "expose: vertices, edges and triplets" in {
    val sourceGraph = graph

    val mappedVertices = sourceGraph.vertices.map {
      case (vertexId, friend) => s"name: ${friend.name}, age: ${friend.age}"
    }.collect()
    val mappedEdges = sourceGraph.edges.map(edge => s"${edge.srcId}-[${edge.attr}]->${edge.dstId}").collect()
    val mappedTriplets = sourceGraph.triplets.map(triplet =>
      s"${triplet.srcId}(${triplet.srcAttr})-[${triplet.attr}]->${triplet.dstId}(${triplet.dstAttr})").collect()

    mappedVertices should have size 3
    mappedVertices should contain allOf("name: A, age: 20", "name: B, age: 21", "name: C, age: 22")
    mappedEdges should have size 2
    mappedEdges should contain allOf("1-[RelationshipAttributes(0,1)]->2", "1-[RelationshipAttributes(0,0)]->3")
    mappedTriplets should have size 2
    mappedTriplets should contain allOf("1(Friend(A,20))-[RelationshipAttributes(0,1)]->2(Friend(B,21))",
      "1(Friend(A,20))-[RelationshipAttributes(0,0)]->3(Friend(C,22))")
  }

  private def graph = {
    val vertices = TestSparkContext.parallelize(
      Seq((1L, Friend("A", 20)), (2L, Friend("B", 21)), (3L, Friend("C", 22)))
    )
    val edges = TestSparkContext.parallelize(
      Seq(Edge(1L, 2L, RelationshipAttributes(0L, 1)), Edge(1L, 3L, RelationshipAttributes(0L, 0)))
    )

    Graph(vertices, edges)
  }

  private def stringifyEdge(edge: Edge[RelationshipAttributes]): String = {
    s"(${edge.srcId})-[${edge.attr.printableAttrs}]->(${edge.dstId})"
  }

  private def stringifyEdges(edges: Array[Edge[RelationshipAttributes]]): String = {
    edges.map(edge => stringifyEdge(edge)).mkString(" / ")
  }


}

object InstancesStore {
  val hashCodes = new scala.collection.mutable.HashSet[Int]()

  def addHashCode(hashCode: Int): Unit = {
    hashCodes.synchronized {
      hashCodes.add(hashCode)
    }
  }
}
case class Friend(name: String, age: Int)
case class RelationshipAttributes(creationTime: Long, maturityLevel: Int) {
  val printableAttrs = s"creationTime: ${creationTime}, maturityLevel: ${maturityLevel}"
}