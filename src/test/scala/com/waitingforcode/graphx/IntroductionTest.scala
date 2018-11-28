package com.waitingforcode.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.graphframes.GraphFrame
import org.scalatest.{FlatSpec, Matchers}


class IntroductionTest extends FlatSpec with Matchers {

  "simple relationship-based code" should "show basic GraphX features" in {
    val sparkContext = SparkContext.getOrCreate(new SparkConf().setAppName("GraphX test").setMaster("local"))
    val parentOfRelationship = "is parent of"

    val people: RDD[(VertexId, String)] =
      sparkContext.parallelize(Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (4L, "user4"), (5L, "user5")))
    val relationships: RDD[Edge[String]] =
      sparkContext.parallelize(Seq(Edge(1L, 2L, parentOfRelationship), Edge(1L, 3L, parentOfRelationship), Edge(2L, 3L, parentOfRelationship),
        Edge(3L, 4L, parentOfRelationship)))
    val graph = Graph(people, relationships)

    val groupedParents = graph.edges.filter(edge => edge.attr == parentOfRelationship)
      .groupBy(edge => edge.srcId).collect()

    val mappedVertices = groupedParents.map(vertexWithEdges => {
      val edges = vertexWithEdges._2
      val destinationVertices = edges.map(edge => edge.dstId)
      (vertexWithEdges._1, destinationVertices)
    }).toMap

    mappedVertices should have size 3
    mappedVertices(1) should have size 2
    mappedVertices(1) should contain allOf(2L, 3L)
    mappedVertices(2) should have size 1
    mappedVertices(2) should contain only 3L
    mappedVertices(3) should have size 1
    mappedVertices(3) should contain only 4L
  }


  "simple relationship-based code" should "be written with GraphFrames" in {
    // In order to run this test you must use Scala 2.11
    val sparkSession = SparkSession.builder().appName("GraphFrames example").master("local").getOrCreate()
    val parentOfRelationship = "is parent of"
    import sparkSession.implicits._
    val people = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (4L, "user4"), (5L, "user5")).toDF("id", "name")
    val relationships = Seq((1L, 2L, parentOfRelationship), (1L, 3L, parentOfRelationship), (2L, 3L, parentOfRelationship),
      (3L, 4L, parentOfRelationship)).toDF("src", "dst", "label")
    val graph = GraphFrame(people, relationships)

    val filteredGraph = graph.filterEdges(s"label = '${parentOfRelationship}'")
    val edgesGroupedBySource = filteredGraph.edges.collect().groupBy(row => row.getAs[Long]("src"))

    val mappedVertices = edgesGroupedBySource.map(vertexWithEdges => {
      val edges = vertexWithEdges._2
      val destinationVertices = edges.map(edge => edge.getAs[Long]("dst"))
      (vertexWithEdges._1, destinationVertices)
    })

    mappedVertices should have size 3
    mappedVertices(1) should have size 2
    mappedVertices(1) should contain allOf(2L, 3L)
    mappedVertices(2) should have size 1
    mappedVertices(2) should contain only 3L
    mappedVertices(3) should have size 1
    mappedVertices(3) should contain only 4L
  }

}
