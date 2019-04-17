package com.waitingforcode.graphx.bipartite

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

class BipartiteGraphTest extends FlatSpec with Matchers {

  val testSparkContext = SparkContext.getOrCreate(new SparkConf().setAppName("GraphX Bipartite graph test").setMaster("local[*]"))
  private val vertices = testSparkContext.parallelize(
    Seq(
      // Top vertices
      (1L, 0), (2L, 0), (3L, 0),
      // Bottom vertices
      (100L, 0), (101L, 0), (102L, 0), (103L, 0), (104L, 0), (105L, 0)
    )
  )
  private val edges = testSparkContext.parallelize(
    Seq(
      // Vertex A
      Edge(1L, 100L, 1), Edge(1L, 101L, 1), Edge(1L, 102L, 1),
      // Vertex B
      Edge(2L, 101L, 1), Edge(2L, 103L, 1),
      // Vertex C
      Edge(3L, 102L, 1), Edge(3L, 103L, 1), Edge(3L, 104L, 1), Edge(3L, 105L, 1)
    )
  )
  private val graph = Graph(vertices, edges)

  it should "stop iteration after reaching the maximal number of iterations" in {
    val verticesWithDegree = graph.triplets.groupBy(triplet => triplet.srcId)
      .map {
        case (userVertexId, edges) => (userVertexId, edges.size)
      }

    val graphWithDegreeDecoratedVertices = Graph(verticesWithDegree, edges)


    val neighborsEdges = graphWithDegreeDecoratedVertices.triplets.groupBy(triplet => triplet.dstId)
      .filter {
        case (_, triplets) => triplets.size > 1
      }
      .flatMap {
        case (itemVertexId, triplets) => {
          val materializedTriplets = triplets.toSeq
          val itemDegree = materializedTriplets.size // degree(v_i)
          val users = materializedTriplets.map(triplet => triplet.srcId)
          val usersDegrees = materializedTriplets.map(triplet => (triplet.srcId, triplet.srcAttr)).toMap
          // build uw pairs
          users.combinations(2)
            .map(neighbours => Seq(
              Edge(neighbours(0), neighbours(1), NeighborsAttributes(usersDegrees(neighbours(0)),
                usersDegrees(neighbours(1)), itemDegree)),
              Edge(neighbours(1), neighbours(0), NeighborsAttributes(usersDegrees(neighbours(1)),
                usersDegrees(neighbours(0)), itemDegree))
            ))
            .flatten
        }
      }

    neighborsEdges.groupBy(edge => (edge.srcId, edge.dstId))
      .foreachPartition(partitionData => {
        partitionData.foreach {
          case ((srcUser, dstUser), commonItemsWithUserDegrees) => {
            val neighborsWeight = RecommendationFunctions.computeNeighborsWeight(commonItemsWithUserDegrees)
            // TODO: try to persist the degree of both users too and not pass it throughout the whole program
            UsersWeightsStore.addWeight(srcUser, dstUser, neighborsWeight)
          }
        }
      })

    UsersWeightsStore.debug

    val weightedValuesPerProduct = graphWithDegreeDecoratedVertices.triplets
      .flatMap(triplet => {
        import scala.collection.JavaConverters._
        val neighbors = UsersWeightsStore.all.asScala.filter(entry => entry._1.contains(s"${triplet.srcId}_"))
        neighbors.map(entry => {
          val neighborId = entry._1.split("_")(1).toLong
          val userNeighbor = entry._2
          val weightedValue = userNeighbor.weight * (1d/userNeighbor.neighborDegree.toDouble)
          (neighborId, triplet.dstId, weightedValue)
        })
      })

    val recommendedProducts = weightedValuesPerProduct.groupBy {
      case (neighborId, _, _) => neighborId
    }.mapPartitions(userSimilarProducts => {
      RecommendationFunctions.computeRecommendedProducts(userSimilarProducts)
    })

    val recommendationResults = recommendedProducts.collect
    val user1Recommendations = recommendationResults.filter(userItems => userItems._1 == 1L)
    user1Recommendations(0)._2 contains allOf((101,0.083), (102,0.042), (103,0.125), (104,0.042), (105,0.042))
    val user2Recommendations = recommendationResults.filter(userItems => userItems._1 == 2L)
    user2Recommendations(0)._2 contains allOf((100,0.083), (101,0.083), (102,0.146), (103,0.063), (104,0.063), (105,0.063))
    val user3Recommendations = recommendationResults.filter(userItems => userItems._1 == 3L)
    user3Recommendations(0)._2 contains allOf((100,0.042), (101,0.104), (102,0.042), (103,0.063))
    println(s"x=${recommendedProducts.collect.mkString("\n")}")
    // expected: x=
    // (1,ArrayBuffer((101,0.08333333333333333), (102,0.041666666666666664), (103,0.125), (104,0.041666666666666664), (105,0.041666666666666664)))
    //(3,ArrayBuffer((100,0.041666666666666664), (101,0.10416666666666666), (102,0.041666666666666664), (103,0.0625)))
    //(2,ArrayBuffer((100,0.08333333333333333), (101,0.08333333333333333), (102,0.14583333333333331), (103,0.0625), (104,0.0625), (105,0.0625)))
  }



}

object RecommendationFunctions {

  def computeNeighborsWeight(commonItemsWithUserDegrees: Iterable[Edge[NeighborsAttributes]]) = {
    var user2Degree = 0d // ==> neighbor edges
    var srcUserEdges = 0d
    var commonItemsSum = 0d
    for (commonItem <- commonItemsWithUserDegrees) {
      srcUserEdges = commonItem.attr.user1Degree
      commonItemsSum += 1d/commonItem.attr.itemDegree

      user2Degree = commonItem.attr.user2Degree
    }
    val neighborWeight = 1/srcUserEdges * commonItemsSum
    NeighborWeightWithDegree(neighborWeight, user2Degree.toInt)
  }

  def computeRecommendedProducts(userSimilarProducts: Iterator[(VertexId, Iterable[(VertexId, VertexId, Double)])]) = {
    userSimilarProducts.map {
      case (userId, productsWeights) => {
        val userTopProducts = productsWeights.groupBy {
          case (_, itemId, _) => itemId
        }.map {
          case (itemId, products) => {
            val weightedValueSum = products.map {
              case (_, _, weight) => weight
            }.sum
            (itemId, BigDecimal(weightedValueSum).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble)
          }
        }
        (userId, userTopProducts.toSeq.sortBy(itemWithValue => itemWithValue._1))
      }
    }
  }
}

case class NeighborsAttributes(user1Degree: Int, user2Degree: Int, itemDegree: Int)

object UsersWeightsStore {

  private val weights = new ConcurrentHashMap[String, NeighborWeightWithDegree]()

  def all = weights

  def addWeight(user1: Long, user2: Long, attribute: NeighborWeightWithDegree) {
    weights.put(s"${user1}_${user2}", attribute)
  }

  def debug = println(s"weights=${weights}")
}

case class NeighborWeightWithDegree(weight: Double, neighborDegree: Int)
