package com.waitingforcode.graphx.loadingwriting

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class SavingGraphTest extends FlatSpec with Matchers with BeforeAndAfter {

  private val TestSparkContext = SparkContext.getOrCreate(new SparkConf().setAppName("Graph saving")
    .setMaster("local[*]"))

  private val TargetObjectFile = new File("/tmp/graphx-saving/object_graph")
  private val TargetTripletsDir = new File("/tmp/graphx-saving/triplets_graph")
  private val TargetJsonDir = new File("/tmp/graphx-saving/json_graph")

  after {
    FileUtils.deleteDirectory(TargetObjectFile)
    FileUtils.deleteDirectory(TargetTripletsDir)
    FileUtils.deleteDirectory(TargetJsonDir)
  }

  "graph edges" should "be saved to an object file" in {
    val edges = TestSparkContext.parallelize(Seq(Edge(1L, 2L, "A"), Edge(1L, 3L, "B"), Edge(2L, 4L, "C")))

    val graph = Graph.fromEdges(edges, "...")

    graph.edges.saveAsObjectFile(TargetObjectFile.getAbsolutePath)

    val edgesFromFile = TestSparkContext.objectFile[Edge[String]](TargetObjectFile.getAbsolutePath)

    val collectedEdges = edgesFromFile.collect()
      .map(edge => s"${edge.srcId}-[${edge.attr}]->${edge.dstId}")
    collectedEdges should have size 3
    collectedEdges should contain allOf("1-[B]->3", "1-[A]->2", "2-[C]->4")
  }

  "graph triplets" should "be saved to an object file" in {
    val edges = TestSparkContext.parallelize(Seq(Edge(1L, 2L, "A"), Edge(1L, 3L, "B"), Edge(2L, 4L, "C")))
    val vertices = TestSparkContext.parallelize(Seq((1L, "AA"), (2L, "BB"), (3L, "CC"), (4L, "DD")))

    val graph = Graph(vertices, edges)

    graph.triplets.saveAsTextFile(TargetTripletsDir.getAbsolutePath)

    val tripletsFromFile = TestSparkContext.textFile(TargetTripletsDir.getAbsolutePath)

    val collectedTriplets = tripletsFromFile.collect()
    collectedTriplets should have size 3
    collectedTriplets should contain allOf("((1,AA),(3,CC),B)", "((1,AA),(2,BB),A)", "((2,BB),(4,DD),C)")
  }

  "graph triplets" should "be saved to a JSON file" in {
    val sparkSession: SparkSession = SparkSession.builder().appName("Graph to JSON").master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._
    val edges = TestSparkContext.parallelize(Seq(Edge(1L, 2L, "A"), Edge(1L, 3L, "B"), Edge(2L, 4L, "C")))
    val vertices = TestSparkContext.parallelize(Seq((1L, "AA"), (2L, "BB"), (3L, "CC"), (4L, "DD")))

    val graph = Graph(vertices, edges)
    val tripletsDataFrame = graph.triplets
      .map(triplet => (triplet.srcId, triplet.srcAttr, triplet.dstId, triplet.dstAttr, triplet.attr))
      .toDF("srcId", "srcAttr", "dstId", "dstAttr", "edgeAttr")
    tripletsDataFrame.write.json(TargetJsonDir.getAbsolutePath)

    val tripletsFromFileAsJson = TestSparkContext.textFile(TargetJsonDir.getAbsolutePath)

    val collectedTriplets = tripletsFromFileAsJson.collect()
    collectedTriplets should have size 3
    collectedTriplets should contain allOf("{\"srcId\":1,\"srcAttr\":\"AA\",\"dstId\":2,\"dstAttr\":\"BB\",\"edgeAttr\":\"A\"}",
      "{\"srcId\":2,\"srcAttr\":\"BB\",\"dstId\":4,\"dstAttr\":\"DD\",\"edgeAttr\":\"C\"}",
      "{\"srcId\":1,\"srcAttr\":\"AA\",\"dstId\":3,\"dstAttr\":\"CC\",\"edgeAttr\":\"B\"}")
  }
}
