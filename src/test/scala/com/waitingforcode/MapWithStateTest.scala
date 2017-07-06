package com.waitingforcode

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Durations, State, StateSpec, StreamingContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MapWithStateTest extends FlatSpec with Matchers with BeforeAndAfter {

  val conf = new SparkConf().setAppName("Spark mapWithState test").setMaster("local[*]")
  var streamingContext: StreamingContext = null
  val dataQueue: mutable.Queue[RDD[Visit]] = new mutable.Queue[RDD[Visit]]()

  before {
    streamingContext = new StreamingContext(conf, Durations.seconds(1))
    streamingContext.checkpoint("/tmp/spark-mapwithstate-test")
  }

  after {
    streamingContext.stop(stopSparkContext = true, stopGracefully = true)
  }

  "expired state" should "help to detect the end of user's visit" in {
    val visits = Seq(
      Visit(1, "home.html", 10), Visit(2, "cart.html", 5), Visit(1, "home.html", 10),
      Visit(2, "address/shipping.html", 10), Visit(2, "address/billing.html", 10)
    )
    visits.foreach(visit => dataQueue += streamingContext.sparkContext.makeRDD(Seq(visit)))

    def handleVisit(key: Long, visit: Option[Visit], state: State[Long]): Option[Any] = {
      (visit, state.getOption()) match {
        case (Some(newVisit), None) => {
          // the 1st visit
          state.update(newVisit.duration)
          None
        }
        case (Some(newVisit), Some(totalDuration)) => {
          // next visit
          state.update(totalDuration + newVisit.duration)
          None
        }
        case (None, Some(totalDuration)) => {
          // last state - timeout occurred and passed
          // value is None in this case
          Some(key, totalDuration)
        }
        case _ => None
      }
    }

    // The state expires 4 seconds after the lasts seen entry for
    // given key. The schedule for our test will look like:
    // user1 -> 0+4, user2 -> 1+4, user1 -> 2+4, user2 -> 3+4, user2 -> 4+4
    val sessionsAccumulator = streamingContext.sparkContext.collectionAccumulator[(Long, Long)]("sessions")
    streamingContext.queueStream(dataQueue)
      .map(visit => (visit.userId, visit))
      .mapWithState(StateSpec.function(handleVisit _).timeout(Durations.seconds(4)))
      .foreachRDD(rdd => {
        val terminatedSessions =
          rdd.filter(_.isDefined).map(_.get.asInstanceOf[(Long, Long)]).collect()
        terminatedSessions.foreach(sessionsAccumulator.add(_))
      })

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(10000)

    println(s"Terminated sessions are ${sessionsAccumulator.value}")
    sessionsAccumulator.value.size shouldEqual(2)
    sessionsAccumulator.value should contain allOf((1, 20), (2, 25))
  }

  "mapWithState" should "help to buffer messages" in {
    // This time mapWithState operation is considered as a buffer
    // Suppose that we want to send user sessions to a data store
    // only at the end of visit (as previously). To do so we need to
    // accumulate all visits
    val visits = Seq(
      Visit(1, "home.html", 10), Visit(2, "cart.html", 5), Visit(1, "cart.html", 10),
      Visit(2, "address/shipping.html", 10), Visit(2, "address/billing.html", 10)
    )
    visits.foreach(visit => dataQueue += streamingContext.sparkContext.makeRDD(Seq(visit)))

    def bufferVisits(key: Long, visit: Option[Visit], state: State[ListBuffer[Visit]]): Option[Seq[Visit]] = {
      val currentVisits = state.getOption().getOrElse(ListBuffer[Visit]())
      if (visit.isDefined) {
        currentVisits.append(visit.get)
        if (currentVisits.length > 2) {
          val returnedVisits = Seq(currentVisits.remove(0), currentVisits.remove(1))
          state.update(currentVisits)
          Some(returnedVisits)
        } else {
          state.update(currentVisits)
          None
        }
      } else {
        // State expired, get all visits
        Some(currentVisits)
      }
    }

    val bufferedSessionsAccumulator = streamingContext.sparkContext
      .collectionAccumulator[Seq[Visit]]("buffered sessions")
    streamingContext.queueStream(dataQueue)
      .map(visit => (visit.userId, visit))
      .mapWithState(StateSpec.function(bufferVisits _).timeout(Durations.seconds(4)))
      .foreachRDD(rdd => {
        val bufferedSessions =
          rdd.filter(_.isDefined).map(_.get).collect()
        bufferedSessions.foreach(visits => bufferedSessionsAccumulator.add(visits))
      })

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(12000)

    val bufferedSessions = bufferedSessionsAccumulator.value
    bufferedSessions.size() shouldEqual(3)
    bufferedSessions.get(0) should contain allOf(Visit(2, "cart.html", 5), Visit(2, "address/billing.html", 10))
    bufferedSessions.get(1) should contain allOf(Visit(1, "home.html", 10), Visit(1, "cart.html", 10))
    bufferedSessions.get(2)(0) shouldBe (Visit(2, "address/shipping.html", 10))
  }

  "shopping sessions" should "be discarded from session tracking thanks to state removal feature" in {
    // Here we want to keep only sessions that
    // don't concern shopping (cart) part. We consider that
    // cart.html is the last possible visited page if the
    // user makes some shopping
    val visits = Seq(
      Visit(1, "home.html", 2), Visit(2, "index.html", 11), Visit(1, "cart.html", 1),
      Visit(1, "forum.html", 1)
    )
    visits.foreach(visit => dataQueue += streamingContext.sparkContext.makeRDD(Seq(visit)))

    def keepNotShoppingSessions(key: Long, visit: Option[Visit], state: State[ListBuffer[Visit]]): Option[Seq[Visit]] = {
      // For simplicity we keep keys infinitely
      if (visit.isDefined) {
        val isCartPage = visit.get.page.contains("cart")
        if (isCartPage) {
          // Discard state for given user
          // However, the discard concerns only previous
          // entries, i.e. it's not persisted for subsequent
          // data for given key
          state.remove()
        } else {
          val currentVisits = state.getOption().getOrElse(ListBuffer[Visit]())
          currentVisits.append(visit.get)
          // update() is mandatory to call
          // otherwise nothing persists
          state.update(currentVisits)
        }
        None
      } else {
        state.getOption()
      }
    }

    val notShoppingSessionsAccumulator = streamingContext.sparkContext
      .collectionAccumulator[Seq[Visit]]("not shopping sessions")
    streamingContext.queueStream(dataQueue)
      .map(visit => (visit.userId, visit))
      .mapWithState(StateSpec.function(keepNotShoppingSessions _).timeout(Durations.seconds(4)))
      .foreachRDD(rdd => {
        val notShoppingSessions =
          rdd.filter(_.isDefined).map(_.get).collect()
        notShoppingSessions.foreach(visits => notShoppingSessionsAccumulator.add(visits))
      })

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(25000)

    val notShoppingSessions = notShoppingSessionsAccumulator.value
    notShoppingSessions.size() shouldEqual(2)
    notShoppingSessions should contain allOf(
      Seq(Visit(1, "forum.html", 1)), Seq(Visit(2, "index.html", 11))
    )
  }

}

case class Visit(userId: Long, page: String, duration: Long)