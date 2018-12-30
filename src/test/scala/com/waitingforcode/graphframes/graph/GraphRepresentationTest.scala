package com.waitingforcode.graphframes.graph

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.graphframes.GraphFrame
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class GraphRepresentationTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession = SparkSession.builder().appName("GraphFrames example").master("local").getOrCreate()
  import sparkSession.implicits._

  override def afterAll(): Unit = {
    sparkSession.stop()
  }

  "GraphFrames" should "create the graph from an in-memory structure" in {
    val parentOfRelationship = "is parent of"
    val people = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (4L, "user4"), (5L, "user5")).toDF("id", "name")
    val relationships = Seq((1L, 2L, parentOfRelationship), (1L, 3L, parentOfRelationship), (2L, 3L, parentOfRelationship),
      (3L, 4L, parentOfRelationship)).toDF("src", "dst", "label")
    val graph = GraphFrame(people, relationships)

    val mappedVertices = graph.vertices.collect().map(row => row.getAs[String]("name"))

    mappedVertices should have size 5
    mappedVertices should contain allOf("user1", "user2", "user3", "user4", "user5")
  }

  "GraphFrames" should "fail creating the vertices without id" in {
    val parentOfRelationship = "is parent of"
    val people = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (4L, "user4"), (5L, "user5")).toDF("user_id", "name")
    val relationships = Seq((1L, 2L, parentOfRelationship), (1L, 3L, parentOfRelationship), (2L, 3L, parentOfRelationship),
      (3L, 4L, parentOfRelationship)).toDF("src", "dst", "label")

    val error = intercept[IllegalArgumentException] {
      GraphFrame(people, relationships)
    }

    error.getMessage() should include("requirement failed: Vertex ID column 'id' missing from vertex DataFrame, which has columns: user_id,name")
  }

  "GraphFrames" should "fail creating the graph when edges don't have src or dst attributes" in {
    val parentOfRelationship = "is parent of"
    val people = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (4L, "user4"), (5L, "user5")).toDF("id", "name")
    val relationships = Seq((1L, 2L, parentOfRelationship), (1L, 3L, parentOfRelationship), (2L, 3L, parentOfRelationship),
      (3L, 4L, parentOfRelationship)).toDF("sourceVertex", "dst", "label")

    val error = intercept[IllegalArgumentException] {
      GraphFrame(people, relationships)
    }

    error.getMessage() should include("requirement failed: Source vertex ID column 'src' missing from edge DataFrame, " +
      "which has columns: sourceVertex,dst,label")
  }

  "GraphFrames" should "fail creating graph with incorrect src and dst values" in {
    val parentOfRelationship = "is parent of"
    val people = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (4L, "user4"), (5L, "user5")).toDF("id", "name")
    val relationships = Seq(("1", "2", parentOfRelationship), ("1", "3", parentOfRelationship), ("2", "3", parentOfRelationship),
      ("3", "4", parentOfRelationship)).toDF("sourceVertex", "dst", "label")

    val error = intercept[IllegalArgumentException] {
      GraphFrame(people, relationships)
    }

    error.getMessage() should include("requirement failed: Source vertex ID column 'src' missing from edge DataFrame, " +
      "which has columns: sourceVertex,dst,label")
  }

  "GraphFrames" should "read the graph from JSON and read it back after mapping the edges" in {
    val usersFile = new File("./users")
    usersFile.deleteOnExit()
    val users =
      """
        |{"id": 1, "name": "user1"}
        |{"id": 2, "name": "user2"}
        |{"id": 3, "name": "user3"}
      """.stripMargin
    FileUtils.writeStringToFile(usersFile, users)
    val friendsFile = new File("./friends")
    friendsFile.deleteOnExit()
    val friends =
      """|{"userFrom": 1, "userTo": 2, "confirmed": true, "isNew": false}
        |{"userFrom": 1, "userTo": 3, "confirmed": true, "isNew": false}""".stripMargin
    FileUtils.writeStringToFile(friendsFile, friends)

    val vertices = sparkSession.read.json(usersFile.getAbsolutePath)
    val edges = sparkSession.read.json(friendsFile.getAbsolutePath)
      .withColumnRenamed("userFrom", "src")
      .withColumnRenamed("userTo", "dst")

    val graph = GraphFrame(vertices, edges)

    val verticesToRestore = new File("./vertices_to_restore")
    verticesToRestore.deleteOnExit()
    graph.vertices.write.mode(SaveMode.Overwrite).json(verticesToRestore.getAbsolutePath)
    val edgesToRestoreFile = new File("./edges_to_restore")
    edgesToRestoreFile.deleteOnExit()
    graph.edges.write.mode(SaveMode.Overwrite).json(edgesToRestoreFile.getAbsolutePath)
    val verticesRestored = sparkSession.read.json(verticesToRestore.getAbsolutePath)
    val edgesRestored = sparkSession.read.json(edgesToRestoreFile.getAbsolutePath)
    val graphRestored = GraphFrame(verticesRestored, edgesRestored)

    val verticesFromRawSource = graph.vertices.collect()
    val verticesFroRestoredGraph = graphRestored.vertices.collect()
    verticesFromRawSource should contain allElementsOf(verticesFroRestoredGraph)
    def mapEdge(row: Row) = s"${row.getAs[Long]("src")} --> ${row.getAs[Long]("dst")}"
    val edgesFromRawSource = graph.edges.collect().map(row => mapEdge(row))
    val edgesFromRestoredGraph = graphRestored.edges.collect().map(row => mapEdge(row))
    edgesFromRawSource should contain allElementsOf(edgesFromRestoredGraph)
  }

}


