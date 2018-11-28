package org.apache.spark.graphx

import org.scalatest.{FlatSpec, Matchers}


class SortTest extends FlatSpec with Matchers {

  "edges" should "be sorted in asc order" in {
    val testEdges: Array[Edge[Int]] = Array(
      Edge(3L, 2L, 1),
      Edge(1L, 4L, 1),
      Edge(1L, 2L, 1),
      Edge(5L, 4L, 6),
      Edge(5L, 2L, 1),
      Edge(5L, 2L, 5),
      Edge(5L, 2L, 2)
    )

    val sortedEdges = testEdges.sorted(Edge.lexicographicOrdering[Int])

    sortedEdges should contain inOrderElementsOf Seq(Edge(1L, 2L, 1), Edge(1L, 4L, 1), Edge(3L, 2L, 1), Edge(5L, 2L, 1),
      Edge(5L, 2L, 5), Edge(5L, 2L, 2), Edge(5L, 4L, 6))
  }

}
