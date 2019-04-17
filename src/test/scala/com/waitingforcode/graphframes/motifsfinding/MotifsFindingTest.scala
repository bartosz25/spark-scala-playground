package com.waitingforcode.graphframes.motifsfinding

import org.apache.spark.sql.{SparkSession, functions}
import org.graphframes.GraphFrame
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class MotifsFindingTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession = SparkSession.builder().appName("GraphFrames motifs finding").master("local").getOrCreate()
  import sparkSession.implicits._

  override def afterAll(): Unit = {
    sparkSession.stop()
  }

  "negated pattern" should "be used to find every user who doesn't follow user3" in {
    val graph = testGraph

    val matchesWithoutNegation = graph.find("(user_1)-[follows]->(user_2)")
      .select(functions.concat($"user_1.name", functions.lit("--[follows]-->"), $"user_2.name").as("match"))
      .where(raw"""follows.label = "follows"""")
    val matchesWithNegation = graph.find("(user_1)-[follows]->(user_2); !(user_2)-[]->(user_1)")
      .select(functions.concat($"user_1.name", functions.lit("--[follows]-->"), $"user_2.name").as("match"))
      .where(raw"""follows.label = "follows"""")

    val followersWithoutNegation = matchesWithoutNegation.collect().map(row => row.getAs[String]("match"))
    followersWithoutNegation should have size 5
    followersWithoutNegation should contain allOf("user1--[follows]-->user3", "user1--[follows]-->user2",
      "user2--[follows]-->user3", "user3--[follows]-->user4", "user4--[follows]-->user3")
    val followersWithNegation = matchesWithNegation.collect().map(row => row.getAs[String]("match"))
    followersWithNegation should have size 3
    followersWithNegation should contain allOf("user1--[follows]-->user3", "user1--[follows]-->user2", "user2--[follows]-->user3")
  }

  "anonymous and named elements" should "be used to find all vertices with followers except user3" in {
    val graph = testGraph

    val matches = graph.find("()-[follows]->(user)")
      .select("user.name")
      .where(raw"""follows.label = "follows" AND user.name != "user3" """)

    val followedUsers = matches.collect().map(row => row.getAs[String]("name"))
    followedUsers should have size 2
    followedUsers should contain allOf("user2", "user4")
  }

  private def testGraph: GraphFrame = {
    val followerRelationship = "follows"
    val people = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (4L, "user4"), (5L, "user5")).toDF("id", "name")
    val relationships = Seq((1L, 2L, followerRelationship), (1L, 3L, followerRelationship), (2L, 3L, followerRelationship),
      (3L, 4L, followerRelationship), (4L, 3L, followerRelationship)).toDF("src", "dst", "label")
    // graph: (user1)---[follows]-->(user2)---[follows]--->(user3)<---[follows]--->(user4)
    //           |                                            ^
    //           ------------------------[follows]------------|
    GraphFrame(people, relationships)
  }

}
