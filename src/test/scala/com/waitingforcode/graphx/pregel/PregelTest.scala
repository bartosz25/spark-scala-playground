package com.waitingforcode.graphx.pregel

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}


class PregelTest  extends FlatSpec with Matchers with BeforeAndAfter {

  before {
    PregelFunctions.IteratedPaths.clear()
  }

  private val testSparkContext = SparkContext.getOrCreate(new SparkConf().setAppName("GraphX Pregel")
    .setMaster("local[*]"))

  private val vertices = testSparkContext.parallelize(
    Seq((1L, ""), (2L, ""), (3L, ""), (4L, ""), (5L, ""))
  )
  private val edges = testSparkContext.parallelize(
    Seq(Edge(1L, 2L, 10), Edge(2, 3, 15), Edge(3, 5, 0), Edge(4, 1, 3))
  )
  private val graph = Graph(vertices, edges)

  behavior of "Pregel"

  // The tested graph looks like:
  //   (1)-[value: 10]->(2)-[value: 15]->(3)-[value: 0]->(5)
  //    ^
  //    |
  // [value: 3]
  //    |
  //   (4)

  it should "stop iteration after reaching the maximal number of iterations" in {
    val graphWithPathVertices = Pregel(graph, "", 2,
      EdgeDirection.In)(PregelFunctions.mergeMessageBeginningSuperstep, PregelFunctions.createMessages,
      PregelFunctions.mergeMessagePartitionLevel)

    val verticesWithAttrs = graphWithPathVertices.vertices.collectAsMap()

    verticesWithAttrs should have size 5
    verticesWithAttrs should contain allOf((2, "3 + 10"), (5, "15 + 0"), (4, ""), (1, "3"), (3, "10 + 15"))
  }

  it should "stop iteration when no more vertices are active" in {
    val graphWithPathVertices = Pregel(graph, "", 10,
      EdgeDirection.In)(PregelFunctions.mergeMessageBeginningSuperstep, PregelFunctions.createMessages,
      PregelFunctions.mergeMessagePartitionLevel)

    val verticesWithAttrs = graphWithPathVertices.vertices.collectAsMap()

    verticesWithAttrs should have size 5
    verticesWithAttrs should contain allOf((2, "3 + 10"), (5, "3 + 10 + 15 + 0"), (4, ""), (1, "3"), (3, "3 + 10 + 15"))
  }

  behavior of "direction parameter"

  it should "send a message to the vertices that received the message in both sides of the edge" in {
    val graphWithPathVertices = Pregel(graph, "", 10,
      EdgeDirection.Both)(PregelFunctions.mergeMessageBeginningSuperstep, PregelFunctions.createMessages,
      PregelFunctions.mergeMessagePartitionLevel)

    val verticesWithAttrs = graphWithPathVertices.vertices.collectAsMap()

    verticesWithAttrs should have size 5
    verticesWithAttrs should contain allOf((2, "3 + 10"), (5, "3 + 10 + 15 + 0"), (4, ""), (1, "3"), (3, "3 + 10 + 15"))
    PregelFunctions.IteratedPaths should have size 10
    PregelFunctions.IteratedPaths.contains("((4,),(1,3),3)") shouldBe false
  }

  it should "send a message to the vertices that received the message in at least one side of the edge" in {
    val graphWithPathVertices = Pregel(graph, "", 10,
      EdgeDirection.Either)(PregelFunctions.mergeMessageBeginningSuperstep, PregelFunctions.createMessages,
      PregelFunctions.mergeMessagePartitionLevel)

    val verticesWithAttrs = graphWithPathVertices.vertices.collectAsMap()

    verticesWithAttrs should have size 5
    verticesWithAttrs should contain allOf((2, "3 + 10"), (5, "3 + 10 + 15 + 0"), (4, ""), (1, "3"), (3, "3 + 10 + 15"))
    PregelFunctions.IteratedPaths should have size 44
    PregelFunctions.IteratedPaths should contain allOf("((4,),(1,),3)", "((4,),(1,3),3)")
    PregelFunctions.IteratedPaths.groupBy(path => path)("((4,),(1,3),3)") should have size 10
  }

}

object PregelFunctions {

  val IteratedPaths = new scala.collection.mutable.ListBuffer[String]()

  def mergeMessageBeginningSuperstep(vertexId: Long, currentValue: String, message: String): String = {
    message
  }

  def mergeMessagePartitionLevel(message1: String, message2: String) = s"${message1} + ${message2}"

  def createMessages(edgeTriplet: EdgeTriplet[String, Int]): Iterator[(VertexId, String)] = {
    val resolvedMessage = if (edgeTriplet.srcAttr.trim.isEmpty) {
      edgeTriplet.attr.toString
    } else {
      edgeTriplet.srcAttr + " + " + edgeTriplet.attr.toString
    }
    IteratedPaths.synchronized {
      IteratedPaths.append(edgeTriplet.toString())
    }
    Iterator((edgeTriplet.dstId, resolvedMessage))
  }

}