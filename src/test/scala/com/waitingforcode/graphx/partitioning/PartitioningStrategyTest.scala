package com.waitingforcode.graphx.partitioning

import org.apache.spark.graphx.PartitionStrategy.{CanonicalRandomVertexCut, EdgePartition1D, EdgePartition2D, RandomVertexCut}
import org.scalatest.{FlatSpec, Matchers}


class PartitioningStrategyTest extends FlatSpec with Matchers {

  behavior of "1 dimension edge partitioning"

  it should "create balanced partitions" in {
    val partitionedEdges = (1 to 100).map(sourceId => EdgePartition1D.getPartition(sourceId, 1, 2))

    val partitioningResult = partitionedEdges.groupBy(nr => nr).mapValues(edges => edges.size)

    partitioningResult should have size 2
    partitioningResult(0) shouldEqual 50
    partitioningResult(1) shouldEqual 50
  }

  it should "create unbalanced partitions" in {
    val partitionsNumber = 2
    // previous test had 1 edge for each of vertices. Here some of vertices have much more edges than the others
    val partitionedEdges = (1 to 100).flatMap(sourceId => {
      if (sourceId % 2 == 0) {
        (0 to 5).map(targetId => EdgePartition1D.getPartition(sourceId, targetId, partitionsNumber))
      } else {
        Seq(EdgePartition1D.getPartition(sourceId, 1, partitionsNumber))
      }
    })

    val partitioningResult = partitionedEdges.groupBy(nr => nr).mapValues(edges => edges.size)

    partitioningResult should have size 2
    partitioningResult(0) shouldEqual 300
    partitioningResult(1) shouldEqual 50
  }

  behavior of "random vertex-cut partitioning"

  it should "create almost balanced partitions" in {
    val partitionedEdges = (1 to 100).map(sourceId => RandomVertexCut.getPartition(sourceId, sourceId+200, 2))

    val partitioningResult = partitionedEdges.groupBy(nr => nr).mapValues(edges => edges.size)

    partitioningResult should have size 2
    partitioningResult(0) shouldEqual 42
    partitioningResult(1) shouldEqual 58
  }

  it should "create unbalanced partitions" in {
    val partitionsNumber = 3
    val partitionedEdges = (1 to 100).flatMap(sourceId => {
      if (sourceId % 2 == 0) {
        (0 to 10).map(_ => RandomVertexCut.getPartition(sourceId, 300+sourceId, partitionsNumber)) ++
          (0 to 10).map(_ => RandomVertexCut.getPartition(300+sourceId, sourceId, partitionsNumber))
      } else {
        Seq(RandomVertexCut.getPartition(sourceId, 200+sourceId, partitionsNumber))
      }
    })

    val partitioningResult = partitionedEdges.groupBy(nr => nr).mapValues(edges => edges.size)

    partitioningResult should have size 3
    partitioningResult(0) shouldEqual 430
    partitioningResult(1) shouldEqual 323
    partitioningResult(2) shouldEqual 397
  }

  behavior of "canonical random vertex-cut partitioning"

  it should "create almost balanced partitions" in {
    val partitionedEdges = (1 to 100).map(sourceId => CanonicalRandomVertexCut.getPartition(sourceId, sourceId+200, 2))

    val partitioningResult = partitionedEdges.groupBy(nr => nr).mapValues(edges => edges.size)

    partitioningResult should have size 2
    partitioningResult(0) shouldEqual 42
    partitioningResult(1) shouldEqual 58
  }

  it should "create more balanced partitions than random vertex-cut" in {
    val partitionsNumber = 3
    val partitionedEdges = (1 to 100).flatMap(sourceId => {
      if (sourceId % 2 == 0) {
        (0 to 10).map(_ => CanonicalRandomVertexCut.getPartition(sourceId, 300+sourceId, partitionsNumber)) ++
          (0 to 10).map(_ => CanonicalRandomVertexCut.getPartition(300+sourceId, sourceId, partitionsNumber))
      } else {
        Seq(CanonicalRandomVertexCut.getPartition(sourceId, 200+sourceId, partitionsNumber))
      }
    })

    val partitioningResult = partitionedEdges.groupBy(nr => nr).mapValues(edges => edges.size)

    partitioningResult should have size 3
    partitioningResult(0) shouldEqual 397 // vs 430 for random vertex-cut
    partitioningResult(1) shouldEqual 345 // vs 323 for random vertex-cut
    partitioningResult(2) shouldEqual 408 // vs 397 for random vertex-cut
  }

  behavior of "2 dimensions edge partitioning"

  it should "perform well for perfect square N" in {
    val partitionsNumber = 9
    val partitionedEdges = (1 to 100).flatMap(sourceId => {
      if (sourceId % 2 == 0) {
        (0 to 10).map(_ => EdgePartition2D.getPartition(sourceId, 300+sourceId, partitionsNumber))
      } else {
        Seq(EdgePartition2D.getPartition(sourceId, 200+sourceId, partitionsNumber))
      }
    })

    val partitioningResult = partitionedEdges.groupBy(nr => nr).mapValues(edges => edges.size)

    partitioningResult should have size 6
    partitioningResult(0) shouldEqual 176
    partitioningResult(2) shouldEqual 17
    partitioningResult(3) shouldEqual 17
    partitioningResult(4) shouldEqual 187
    partitioningResult(8) shouldEqual 187
  }

  it should "perform a little bit worse for not perfect square N" in {
    val partitionsNumber = 8
    val partitionedEdges = (1 to 100).flatMap(sourceId => {
      if (sourceId % 2 == 0) {
        (0 to 10).map(_ => EdgePartition2D.getPartition(sourceId, 300+sourceId, partitionsNumber))
      } else {
        Seq(EdgePartition2D.getPartition(sourceId, 200+sourceId, partitionsNumber))
      }
    })

    val partitioningResult = partitionedEdges.groupBy(nr => nr).mapValues(edges => edges.size)
    partitioningResult should have size partitionsNumber
    partitioningResult(0) shouldEqual 92
    partitioningResult(1) shouldEqual 92
    partitioningResult(2) shouldEqual 103
    partitioningResult(3) shouldEqual 53
    partitioningResult(4) shouldEqual 63
    partitioningResult(5) shouldEqual 52
    partitioningResult(6) shouldEqual 132
    partitioningResult(7) shouldEqual 13
  }

  it should "guarantee each vertex on only 2 partitions" in {
    val partitionsNumber = 4
    val partitionedEdges = (1 to 100).flatMap(sourceId => {
      if (sourceId % 2 == 0) {
        (0 to 10).flatMap(_ => {
          val destinationId = 300+sourceId
          val partition = EdgePartition2D.getPartition(sourceId, destinationId, partitionsNumber)
          Seq((sourceId, partition), (destinationId, partition))
        })
      } else {
        val destinationId = 200+sourceId
        val partition = EdgePartition2D.getPartition(sourceId, destinationId, partitionsNumber)
        Seq((sourceId, partition), (destinationId, partition))
      }
    })

    val verticesOnPartitions = partitionedEdges.groupBy(vertexWithPartition => vertexWithPartition._1)
      .mapValues(vertexWithPartitions => vertexWithPartitions.map(vertexIdPartitionId => vertexIdPartitionId._2).distinct.size)
    val verticesWith1Partition = verticesOnPartitions.filter(vertexIdPartitions => vertexIdPartitions._2 == 1).size
    val verticesWith2Partitions = verticesOnPartitions.filter(vertexIdPartitions => vertexIdPartitions._2 == 2).size
    verticesOnPartitions.size shouldEqual (verticesWith1Partition + verticesWith2Partitions)
  }

  /**
    * TODO: it's for the image of matrix
    *     println(partitioningResult)

    val edges = Seq((1, 2), (1, 3), (2, 4), (4, 5), (4, 6), (4, 7), (7, 8), (8, 9))
    val partitions = edges.map {
      case (sourceId, destinationId) => s"${sourceId}->${destinationId} => ${EdgePartition2D.getPartition(sourceId, destinationId, 3)}"
    }
    println(partitions.mkString("\n"))

    */
}
