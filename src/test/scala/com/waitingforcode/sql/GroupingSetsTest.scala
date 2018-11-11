package com.waitingforcode.sql


import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class GroupingSetsTest extends FlatSpec with Matchers {

  private val TestSparkSession = SparkSession.builder().appName("Spark grouping sets tests").master("local[*]")
    .getOrCreate()

  import TestSparkSession.implicits._

  private val Team1Label = "Team1"
  private val Team2Label = "Team2"

  private val TestedDataSet = Seq(
    ("pl", "Scala", Team1Label, 10), ("pl", "Java", Team1Label, 1), ("pl", "C++", Team2Label, 2),
    ("us", "Scala", Team2Label,15), ("us", "Java", Team2Label,3),
    ("fr", "Scala", Team2Label,5), ("fr", "Java", Team2Label,9)
  ).toDF("country", "language", "team", "projects_number")

  case class GroupingSetResult(country: String, language: String, team: String, aggregationValue: Long)

  private def mapRowToGroupingSetResult(row: Row) = GroupingSetResult(row.getAs[String]("country"),
    row.getAs[String]("language"), row.getAs[String]("team"), row.getAs[Long]("sum(projects_number)"))

  "rollup" should "compute aggregates by country, language and team" in {
    val rollupResults = TestedDataSet.rollup($"country", $"language", $"team").sum("projects_number")

    val collectedResults = rollupResults.collect().map(row => mapRowToGroupingSetResult(row)).toSeq

    collectedResults should have size 18
    collectedResults should contain allOf(
      GroupingSetResult("fr", "Java", "Team2", 9), GroupingSetResult("pl", "C++", "Team2", 2), GroupingSetResult("us", "Java", "Team2", 3),
      GroupingSetResult("fr", "Scala", "Team2", 5), GroupingSetResult("pl", "Java", "Team1", 1), GroupingSetResult("us", "Scala", "Team2", 15),
      GroupingSetResult("pl", "Scala", "Team1", 10),
      GroupingSetResult("pl", "Java", null, 1), GroupingSetResult("us", "Java", null, 3), GroupingSetResult("pl", "Scala", null, 10),
      GroupingSetResult("us", "Scala", null, 15), GroupingSetResult("fr", "Java", null, 9), GroupingSetResult("fr", "Scala", null, 5),
      GroupingSetResult("pl", "C++", null, 2),
      GroupingSetResult("pl", null, null, 13),GroupingSetResult("fr", null, null, 14), GroupingSetResult("us", null, null, 18),
      GroupingSetResult(null, null, null,45)
      )
    rollupResults.explain()
    /**
      * Execution plan is:
      == Physical Plan ==
      *(2) HashAggregate(keys=[country#42, language#43, team#44, spark_grouping_id#38], functions=[sum(cast(projects_number#12 as bigint))])
      +- Exchange hashpartitioning(country#42, language#43, team#44, spark_grouping_id#38, 200)
         +- *(1) HashAggregate(keys=[country#42, language#43, team#44, spark_grouping_id#38], functions=[partial_sum(cast(projects_number#12 as bigint))])
            +- *(1) Expand [List(projects_number#12, country#39, language#40, team#41, 0), List(projects_number#12, country#39, language#40, null, 1), List(projects_number#12, country#39, null, null, 3), List(projects_number#12, null, null, null, 7)], [projects_number#12, country#42, language#43, team#44, spark_grouping_id#38]
               +- LocalTableScan [projects_number#12, country#39, language#40, team#41]
      */
  }

  "cube" should "compute aggregates by country, language and team" in {
    val cubeResults = TestedDataSet.cube($"country",  $"language", $"team").sum("projects_number")

    val collectedResults = cubeResults.collect().map(row => mapRowToGroupingSetResult(row)).toSeq

    collectedResults should have size 32
    collectedResults should contain allOf(
      // country, language, team
      GroupingSetResult("pl", "Scala", "Team1", 10), GroupingSetResult("fr", "Java", "Team2", 9), GroupingSetResult("pl", "C++", "Team2", 2),
      GroupingSetResult("us", "Java", "Team2", 3), GroupingSetResult("fr", "Scala", "Team2", 5), GroupingSetResult("pl", "Java", "Team1", 1),
      GroupingSetResult("us", "Scala", "Team2", 15),
      // country, language
      GroupingSetResult("us", "Java", null, 3), GroupingSetResult("pl", "Java", null, 1), GroupingSetResult("pl", "Scala", null, 10),
      GroupingSetResult("fr", "Java", null, 9), GroupingSetResult("fr", "Scala", null, 5), GroupingSetResult("us", "Scala", null, 15),
      GroupingSetResult("pl", "C++", null, 2),
      // country, team
      GroupingSetResult("us", null, "Team2", 18), GroupingSetResult("pl", null, "Team2", 2), GroupingSetResult("pl", null, "Team1", 11),
      GroupingSetResult("fr", null, "Team2", 14),
      // country
      GroupingSetResult("pl", null, null, 13), GroupingSetResult("us", null, null, 18), GroupingSetResult("fr", null, null, 14),
      // language, team
      GroupingSetResult(null, "Java", "Team2", 12), GroupingSetResult(null, "C++", "Team2", 2),
      GroupingSetResult(null, "Scala", "Team1", 10), GroupingSetResult(null, "Java", "Team1", 1), GroupingSetResult(null, "Scala", "Team2", 20),
      // language
      GroupingSetResult(null, "Scala", null, 30), GroupingSetResult(null, "Java", null, 13), GroupingSetResult(null, "C++", null, 2),
      // team
      GroupingSetResult(null, null, "Team1", 11),  GroupingSetResult(null, null, "Team2", 34),
      // total
      GroupingSetResult(null, null, null, 45)
      )
    cubeResults.explain()

    /**
      * Execution plan is:
      == Physical Plan ==
      *(2) HashAggregate(keys=[country#42, language#43, team#44, spark_grouping_id#38], functions=[sum(cast(projects_number#12 as bigint))])
      +- Exchange hashpartitioning(country#42, language#43, team#44, spark_grouping_id#38, 200)
         +- *(1) HashAggregate(keys=[country#42, language#43, team#44, spark_grouping_id#38], functions=[partial_sum(cast(projects_number#12 as bigint))])
            +- *(1) Expand [List(projects_number#12, country#39, language#40, team#41, 0), List(projects_number#12, country#39, language#40, null, 1), List(projects_number#12, country#39, null, team#41, 2), List(projects_number#12, country#39, null, null, 3), List(projects_number#12, null, language#40, team#41, 4), List(projects_number#12, null, language#40, null, 5), List(projects_number#12, null, null, team#41, 6), List(projects_number#12, null, null, null, 7)], [projects_number#12, country#42, language#43, team#44, spark_grouping_id#38]
               +- LocalTableScan [projects_number#12, country#39, language#40, team#41]
      */
  }

}
