package com.waitingforcode.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Integration test example on Spark. It shows the use
  * of 3 concepts described in blog post about unit and
  * integration tests in Spark: shared context, DSL and custom matchers.
  */
class IntegrationTestExampleTest extends FlatSpec with Matchers with BeforeAndAfter with WithMethodScopedSparkContext {

  before {
    withLocalContext("Integration test example")
  }

  "RDD representing user sessions" should "be grouped by key and collected in the action" in {
    val rdd = (UserSessionDsl sessionsData sparkContext) > UserSession(1, "home.html", 30) >
      UserSession(2, "home.html", 45) > UserSession(2, "catalog.html", 10) > UserSession(2, "exit.html", 5) >
      UserSession(1, "cart.html", 10) > UserSession(1, "payment.html", 10) toRdd

    val wholeSessions = rdd.map(session => (session.userId, session.timeInSec))
      .reduceByKey((time1, time2) => time1 + time2)
      .map((userStatPair) => UserVisit(userStatPair._1, userStatPair._2))
      .collect()

    UserVisitMatcher.user(wholeSessions, 1).spentTotalTimeOnSite(50)
    UserVisitMatcher.user(wholeSessions, 2).spentTotalTimeOnSite(60)
  }

}


/**
  * Trait that can be used to facilitate work with SparkContext lifecycle
  * in tests.
  * It's only for illustration purposes. To your production
  * application you can consider to use Spark Testing Base from https://github.com/holdenk/spark-testing-base
  */
trait WithMethodScopedSparkContext {

  var sparkContext:SparkContext = null

  def withLocalContext(appName: String) = {
    if (sparkContext != null) {
      sparkContext.stop()
    }
    val conf = new SparkConf().setAppName(appName).setMaster("local")
    sparkContext = SparkContext.getOrCreate(conf)
  }
}


/**
  * Below code shows sample custom DSL used to inject data
  * representing user sessions for batch processing.
  */
class UserSessionDsl(sparkContext: SparkContext) {

  val sessions:ListBuffer[UserSession] = new ListBuffer()

  def >(userSession: UserSession): UserSessionDsl = {
    sessions.append(userSession)
    this
  }

  def toRdd(): RDD[UserSession] = {
    sparkContext.parallelize(sessions)
  }
}

object UserSessionDsl {
  def sessionsData(sparkContext: SparkContext): UserSessionDsl = {
    new UserSessionDsl(sparkContext)
  }
}

case class UserSession(userId: Long, page: String, timeInSec: Long)

/**
  * Sample code used to build a matcher.
  */
case class UserVisit(userId: Long, totalSpentTimeInSec: Long)

class UserVisitMatcher(visits: Seq[UserVisit], userId: Long) {

  val userVisitOption = visits.find(_.userId == userId)
  assert(userVisitOption.isDefined)
  val userVisit = userVisitOption.get

  def spentTotalTimeOnSite(expectedTime: Long): UserVisitMatcher = {
    assert(userVisit.totalSpentTimeInSec == expectedTime)
    this
  }


}

object UserVisitMatcher {
  def user(visits: Seq[UserVisit], userId: Long): UserVisitMatcher = {
    new UserVisitMatcher(visits, userId)
  }
}