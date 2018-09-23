package com.waitingforcode.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class WindowFunctionsTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val sparkSession: SparkSession = SparkSession.builder().appName("Window functions test").master("local[*]")
    .getOrCreate()

  // Stats are coming from https://www.fifa.com/worldcup/teams/team/43935/ (and other teams)
  import sparkSession.implicits._
  private val WorldCupPlayers = Seq(
    Player("Harry Kane", "England", 6, 0),
    Player("Cristiano Ronaldo", "Portugal", 4, 1),
    Player("Antoine Griezmann", "France", 4, 2),
    Player("Romelu Lukaku", "Belgium", 4, 1),
    Player("Denis Cheryshev", "Russia", 4, 0),
    Player("Kylian MBappe", "France", 4, 0),
    Player("Eden Hazard", "Belgium", 3, 2),
    Player("Artem Dzuyba", "Russia", 3, 2),
    Player("John Stones", "England", 2, 0),
    Player("Kevin De Bruyne", "Belgium", 1, 3),
    Player("Aleksandr Golovin", "Russia", 1, 2),
    Player("Paul Pogba", "France", 1, 0),
    Player("Pepe", "Portugal", 1, 0),
    Player("Ricardo Quaresma", "Portugal", 1, 0),
    Player("Dele Alli", "England", 1, 0)
  ).toDF()

  override def afterAll(): Unit = {
    sparkSession.stop()
  }

  behavior of "window function"

  it should "link to the next scorer in the team" in {
    val theBestScorersWindow = Window.partitionBy($"team").orderBy($"goals".desc)

    // it'll return a value x places after current row
    val theBestScorers = lead($"name", 1, "-").over(theBestScorersWindow)

    // for the simplicity, the case when several player have
    val scorers = WorldCupPlayers.select($"*", theBestScorers.as("next_scorer")).map(row => (
      row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[String]("next_scorer")
      )).collect()

    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Denis Cheryshev", 4, "Artem Dzuyba"), ("Artem Dzuyba", 3, "Aleksandr Golovin"), ("Aleksandr Golovin",1, "-"),
      ("Antoine Griezmann", 4, "Kylian MBappe"), ("Kylian MBappe", 4, "Paul Pogba"), ("Paul Pogba", 1, "-"),
      ("Romelu Lukaku", 4, "Eden Hazard"), ("Eden Hazard", 3, "Kevin De Bruyne"), ("Kevin De Bruyne", 1, "-"),
      ("Harry Kane", 6, "John Stones"), ("John Stones", 2, "Dele Alli"), ("Dele Alli", 1, "-"),
      ("Cristiano Ronaldo", 4, "Pepe"), ("Pepe", 1, "Ricardo Quaresma"), ("Ricardo Quaresma", 1, "-")
      )
  }

  it should "link to the previous scorer in the team" in {
    val theBestScorersWindow = Window.partitionBy($"team").orderBy($"goals".desc)

    // it'll return "lag" value x places before current row - it helps to build a sequence between rows
    val theBestScorers = lag($"name", 1, "-").over(theBestScorersWindow)

    // for the simplicity, the case when several player have
    val scorers = WorldCupPlayers.select($"*", theBestScorers.as("previous_scorer")).map(row => (
      row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[String]("previous_scorer")
      )).collect()

    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Denis Cheryshev", 4, "-"), ("Artem Dzuyba", 3, "Denis Cheryshev"), ("Aleksandr Golovin", 1, "Artem Dzuyba"),
      ("Antoine Griezmann", 4, "-"), ("Kylian MBappe", 4, "Antoine Griezmann"), ("Paul Pogba", 1, "Kylian MBappe"),
      ("Romelu Lukaku", 4, "-"), ("Eden Hazard", 3, "Romelu Lukaku"), ("Kevin De Bruyne", 1, "Eden Hazard"),
      ("Harry Kane", 6, "-"), ("John Stones", 2, "Harry Kane"), ("Dele Alli", 1, "John Stones"),
      ("Cristiano Ronaldo", 4, "-"), ("Pepe", 1, "Cristiano Ronaldo"), ("Ricardo Quaresma", 1, "Pepe")
      )
  }

  it should "rank goals per team" in {
    val goalsPerTeam = Window.partitionBy($"team").orderBy($"goals".desc)

    // the difference with rank() is that dense_rank() keeps incremental number if two or more rows
    // are in the same position. For instance rank() will produce a result like (1, 2, 2, 4) while dense_rank() like
    // (1, 2, 2, 3)
    val goalsPerPlayerInTeam = rank().over(goalsPerTeam)

    val scorers = WorldCupPlayers.select($"*", goalsPerPlayerInTeam.as("scorer_position")).map(row => (
      row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[Int]("scorer_position")
      )).collect()

    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Denis Cheryshev", 4, 1), ("Artem Dzuyba", 3, 2), ("Aleksandr Golovin", 1, 3),
      ("Antoine Griezmann", 4, 1), ("Kylian MBappe", 4, 1), ("Paul Pogba", 1, 3),
      ("Romelu Lukaku", 4, 1), ("Eden Hazard", 3, 2), ("Kevin De Bruyne", 1, 3),
      ("Harry Kane", 6, 1), ("John Stones", 2, 2), ("Dele Alli", 1, 3),
      ("Cristiano Ronaldo", 4, 1), ("Pepe", 1, 2), ("Ricardo Quaresma", 1, 2)
      )
  }

  it should "dense rank goals per team" in {
    val goalsPerTeam = Window.partitionBy($"team").orderBy($"goals".desc)

    // the difference with rank() is that dense_rank() keeps incremental number if two or more rows
    // are in the same position. For instance rank() will produce a result like (1, 2, 2, 4) while dense_rank() like
    // (1, 2, 2, 3)
    val goalsPerPlayerInTeam = dense_rank().over(goalsPerTeam)

    val scorers = WorldCupPlayers.select($"*", goalsPerPlayerInTeam.as("scorer_position")).map(row => (
      row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[Int]("scorer_position")
      )).collect()

    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Denis Cheryshev", 4, 1), ("Artem Dzuyba", 3, 2), ("Aleksandr Golovin", 1, 3),
      ("Antoine Griezmann", 4, 1), ("Kylian MBappe", 4, 1), ("Paul Pogba", 1, 2),
      ("Romelu Lukaku", 4, 1), ("Eden Hazard", 3, 2), ("Kevin De Bruyne", 1, 3),
      ("Harry Kane", 6, 1), ("John Stones", 2, 2), ("Dele Alli", 1, 3),
      ("Cristiano Ronaldo", 4, 1), ("Pepe", 1, 2), ("Ricardo Quaresma", 1, 2)
      )
  }

  it should "return row number for each grouped player" in {
    val goalsPerTeam = Window.partitionBy($"team").orderBy($"goals".desc)

    val goalsPerPlayerInTeam = row_number().over(goalsPerTeam)

    val scorers = WorldCupPlayers.select($"*", goalsPerPlayerInTeam.as("nr")).map(row => (
      row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[Int]("nr")
      )).collect()

    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Denis Cheryshev", 4, 1), ("Artem Dzuyba", 3, 2), ("Aleksandr Golovin", 1, 3),
      ("Antoine Griezmann", 4, 1), ("Kylian MBappe", 4, 2), ("Paul Pogba", 1, 3),
      ("Romelu Lukaku", 4, 1), ("Eden Hazard", 3, 2), ("Kevin De Bruyne", 1, 3),
      ("Harry Kane", 6, 1), ("John Stones", 2, 2), ("Dele Alli", 1, 3),
      ("Cristiano Ronaldo", 4, 1), ("Pepe", 1, 2), ("Ricardo Quaresma", 1, 3)
      )
  }

  it should "put players in 2 groups depending on scored goals" in {
    val theBestScorersWindow = Window.partitionBy($"goals").orderBy($"name".asc)

    val groups = 2
    val theBestScorers = ntile(groups).over(theBestScorersWindow)

    val scorers = WorldCupPlayers.select($"*", theBestScorers.as("group")).map(row => (
      row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[Int]("group")
      )).collect()

    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Aleksandr Golovin", 1, 1), ("Dele Alli", 1, 1), ("Kevin De Bruyne", 1, 1), ("Paul Pogba", 1, 2), ("Pepe", 1, 2),
      ("Ricardo Quaresma", 1, 2), ("Harry Kane", 6, 1), ("Artem Dzuyba", 3, 1), ("Eden Hazard", 3, 2),
      ("Antoine Griezmann", 4, 1), ("Cristiano Ronaldo", 4, 1), ("Denis Cheryshev", 4, 1), ("Kylian MBappe", 4, 2),
      ("Romelu Lukaku", 4, 2), ("John Stones", 2, 1)
      )
  }

  it should "return average goals per team" in {
    val theBestScorersWindow = Window.partitionBy($"team").orderBy($"team")

    val avgGoalsPerPlayerInTeam = avg($"goals").over(theBestScorersWindow)

    val scorers = WorldCupPlayers.select($"*", avgGoalsPerPlayerInTeam.as("avg_goals")).map(row => (
      row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[Double]("avg_goals")
      )).collect()

    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Denis Cheryshev", 4, 2.6666666666666665), ("Artem Dzuyba", 3, 2.6666666666666665),
      ("Aleksandr Golovin", 1, 2.6666666666666665),
      ("Antoine Griezmann", 4, 3.0), ("Kylian MBappe", 4, 3.0), ("Paul Pogba", 1, 3.0),
      ("Romelu Lukaku", 4, 2.6666666666666665), ("Eden Hazard", 3, 2.6666666666666665),
      ("Kevin De Bruyne", 1, 2.6666666666666665),
      ("Harry Kane", 6, 3.0), ("John Stones", 2, 3.0), ("Dele Alli", 1, 3.0),
      ("Cristiano Ronaldo", 4, 2.0), ("Pepe", 1, 2.0), ("Ricardo Quaresma", 1, 2.0)
      )
  }

  it should "return the number of rows below each scorer" in {
    val theBestScorersWindow = Window.partitionBy($"team").orderBy($"goals".desc)

    // cume_dist() => returns the number of rows before current row (current row included)
    val theBestScorers = cume_dist().over(theBestScorersWindow)

    val scorers = WorldCupPlayers.select($"*", theBestScorers.as("players_below")).map(row => {
      (row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[Double]("players_below"))
    }).collect()

    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Denis Cheryshev", 4, 0.3333333333333333), ("Artem Dzuyba", 3, 0.6666666666666666), ("Aleksandr Golovin", 1, 1.0),
      ("Antoine Griezmann", 4, 0.6666666666666666), ("Kylian MBappe", 4, 0.6666666666666666), ("Paul Pogba", 1, 1.0),
      ("Romelu Lukaku", 4, 0.3333333333333333), ("Eden Hazard", 3, 0.6666666666666666), ("Kevin De Bruyne", 1, 1.0),
      ("Harry Kane", 6, 0.3333333333333333), ("John Stones", 2, 0.6666666666666666), ("Dele Alli", 1, 1.0),
      ("Cristiano Ronaldo", 4, 0.3333333333333333), ("Pepe", 1, 1.0), ("Ricardo Quaresma", 1, 1.0)
      )
  }

  it should "compute relative rank for scorer within window frame" in {
    val theBestScorersWindow = Window.partitionBy($"team").orderBy($"goals".desc)

    // percent_rank() => returns relative rank in %, i.e. how far given row is from the end of rows within
    // window partition
    val theBestScorers = percent_rank().over(theBestScorersWindow)

    val scorers = WorldCupPlayers.select($"*", theBestScorers.as("relative_position_from_the_best")).map(row => {
      (row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[Double]("relative_position_from_the_best"))
    }).collect()

    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Denis Cheryshev", 4, 0.0), ("Artem Dzuyba", 3, 0.5), ("Aleksandr Golovin", 1, 1.0),
      ("Antoine Griezmann", 4, 0.0), ("Kylian MBappe", 4, 0.0), ("Paul Pogba", 1, 1.0),
      ("Romelu Lukaku", 4, 0.0), ("Eden Hazard", 3, 0.5), ("Kevin De Bruyne", 1, 1.0),
      ("Harry Kane", 6, 0.0), ("John Stones", 2, 0.5), ("Dele Alli", 1, 1.0),
      ("Cristiano Ronaldo", 4, 0.0), ("Pepe", 1, 0.5), ("Ricardo Quaresma", 1, 0.5)
      )
  }


  it should "take whole frame with unbounded functions added in Apache Spark 2.3.0" in {
    val theBestScorersWindow = Window.partitionBy($"team").orderBy($"goals".desc)
      .rangeBetween(unboundedPreceding(), unboundedFollowing())

    val theBestScorers = count($"name").over(theBestScorersWindow)

    val scorers = WorldCupPlayers.select($"*", theBestScorers.as("all_players_in_team")).map(row => {
      (row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[Long]("all_players_in_team"))
    }).collect()

    println(s"scorers=${scorers.mkString(",")}")
    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Denis Cheryshev", 4, 3), ("Artem Dzuyba", 3, 3), ("Aleksandr Golovin", 1, 3),
      ("Antoine Griezmann", 4, 3), ("Kylian MBappe", 4, 3), ("Paul Pogba", 1, 3),
      ("Romelu Lukaku", 4, 3), ("Eden Hazard", 3, 3), ("Kevin De Bruyne", 1, 3),
      ("Harry Kane", 6, 3), ("John Stones", 2, 3), ("Dele Alli", 1, 3),
      ("Cristiano Ronaldo", 4, 3), ("Pepe", 1, 3), ("Ricardo Quaresma", 1, 3)
      )
  }

  it should "take a frame bounded by current row with new function added in Apache Spark 2.3.0" in {
    val theBestScorersWindow = Window.partitionBy($"team").orderBy($"goals".desc)
      .rangeBetween(currentRow(), unboundedFollowing())

    val theBestScorers = count($"name").over(theBestScorersWindow)

    val scorers = WorldCupPlayers.select($"*", theBestScorers.as("all_players_in_team")).map(row => {
      (row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[Long]("all_players_in_team"))
    }).collect()

    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Denis Cheryshev", 4, 3), ("Artem Dzuyba", 3, 2), ("Aleksandr Golovin", 1, 1),
      ("Antoine Griezmann", 4, 3), ("Kylian MBappe", 4, 3), ("Paul Pogba", 1, 1),
      ("Romelu Lukaku", 4, 3), ("Eden Hazard", 3, 2), ("Kevin De Bruyne", 1, 1),
      ("Harry Kane", 6, 3), ("John Stones", 2, 2), ("Dele Alli", 1, 1),
      ("Cristiano Ronaldo", 4, 3), ("Pepe", 1, 2), ("Ricardo Quaresma", 1, 2)
      )
  }

  it should "apply custom row frame for average aggregation" in {
    val theBestScorersWindow = Window.partitionBy($"team").orderBy($"goals".desc).rowsBetween(-1, 1)

    val theBestScorers = avg($"goals").over(theBestScorersWindow)

    val scorers = WorldCupPlayers.select($"*", theBestScorers.as("rows_avg")).map(row => (
      row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[Double]("rows_avg")
      )).collect()

    // For the first row we'll never get the row before (-1). The same applies for the last row that will never
    // have an access to the next row (1). It's why the averages aren't the same for all players
    // It's a kind of sliding window according to physical offsets
    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Denis Cheryshev", 4, 3.5), ("Artem Dzuyba", 3, 2.6666666666666665), ("Aleksandr Golovin", 1, 2.0),
      ("Antoine Griezmann", 4, 4.0), ("Kylian MBappe", 4, 3.0), ("Paul Pogba", 1, 2.5),
      ("Romelu Lukaku", 4, 3.5), ("Eden Hazard", 3, 2.6666666666666665), ("Kevin De Bruyne", 1, 2.0),
      ("Harry Kane", 6, 4.0), ("John Stones", 2, 3.0), ("Dele Alli", 1, 1.5),
      ("Cristiano Ronaldo", 4, 2.5), ("Pepe", 1, 2.0), ("Ricardo Quaresma", 1, 1.0)
      )
  }

  it should "apply custom range frame for sum goals" in {
    val theBestScorersWindow = Window.partitionBy($"team").orderBy($"goals".asc)

    val range = theBestScorersWindow.rangeBetween(-2, 1)

    val theBestScorers = sum($"goals").over(range)

    // for the simplicity, the case when several player have
    val scorers = WorldCupPlayers.select($"*", theBestScorers.as("range_sum")).map(row => (
      row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[Long]("range_sum")
      )).collect()

    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Aleksandr Golovin", 1, 1), ("Artem Dzuyba", 3, 8), ("Denis Cheryshev", 4, 7),
      ("Paul Pogba", 1, 1), ("Antoine Griezmann", 4, 8), ("Kylian MBappe", 4, 8),
      ("Kevin De Bruyne", 1, 1), ("Eden Hazard", 3, 8), ("Romelu Lukaku", 4, 7),
      ("Dele Alli", 1, 3), ("John Stones", 2, 3), ("Harry Kane", 6, 6),
      ("Pepe", 1, 2), ("Ricardo Quaresma", 1, 2), ("Cristiano Ronaldo", 4, 4)
      )
  }

  "entire partition frame" should "be created with unbounding preceeding and succeeding expressions" in {
    val theBestScorersWindow = Window.partitionBy($"team").orderBy($"goals".desc)
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    val theBestScorers = avg($"goals").over(theBestScorersWindow)

    val scorers = WorldCupPlayers.select($"*", theBestScorers.as("rows_avg")).map(row => (
      row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[Double]("rows_avg")
      )).collect()

    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Denis Cheryshev", 4, 2.6666666666666665), ("Artem Dzuyba", 3, 2.6666666666666665), ("Aleksandr Golovin", 1, 2.6666666666666665),
      ("Antoine Griezmann", 4, 3.0), ("Kylian MBappe", 4, 3.0), ("Paul Pogba", 1, 3.0),
      ("Romelu Lukaku", 4, 2.6666666666666665), ("Eden Hazard", 3, 2.6666666666666665), ("Kevin De Bruyne", 1, 2.6666666666666665),
      ("Harry Kane", 6, 3.0), ("John Stones", 2, 3.0), ("Dele Alli", 1, 3.0),
      ("Cristiano Ronaldo", 4, 2.0), ("Pepe", 1, 2.0), ("Ricardo Quaresma", 1, 2.0)
      )
  }

  "growing frame" should "be created with unbounded preceding and current row expression" in {
    val theBestScorersWindow = Window.partitionBy($"team").orderBy($"goals".desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val theBestScorers = avg($"goals").over(theBestScorersWindow)

    val scorers = WorldCupPlayers.select($"*", theBestScorers.as("rows_avg")).map(row => (
      row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[Double]("rows_avg")
      )).collect()

    println(s"scorers=${scorers.mkString(",")}")
    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Denis Cheryshev", 4, 4.0), ("Artem Dzuyba", 3, 3.5), ("Aleksandr Golovin", 1, 2.6666666666666665),
      ("Antoine Griezmann", 4, 4.0), ("Kylian MBappe", 4, 4.0), ("Paul Pogba", 1, 3.0),
      ("Romelu Lukaku", 4, 4.0), ("Eden Hazard", 3, 3.5), ("Kevin De Bruyne", 1, 2.6666666666666665),
      ("Harry Kane", 6, 6.0), ("John Stones", 2, 4.0), ("Dele Alli", 1, 3.0),
      ("Cristiano Ronaldo", 4, 4.0), ("Pepe", 1, 2.5), ("Ricardo Quaresma", 1, 2.0)
      )
  }

  "shrinking frame" should "be created with current row and unbounded following expression" in {
    val theBestScorersWindow = Window.partitionBy($"team").orderBy($"goals".desc)
      .rowsBetween(Window.currentRow, Window.unboundedFollowing)

    val theBestScorers = avg($"goals").over(theBestScorersWindow)

    val scorers = WorldCupPlayers.select($"*", theBestScorers.as("rows_avg")).map(row => (
      row.getAs[String]("name"), row.getAs[Int]("goals"), row.getAs[Double]("rows_avg")
      )).collect()

    println(s"scorers=${scorers.mkString(",")}")
    scorers should have size WorldCupPlayers.count()
    scorers should contain allOf(
      ("Denis Cheryshev", 4, 2.6666666666666665), ("Artem Dzuyba", 3, 2.0), ("Aleksandr Golovin", 1, 1.0),
      ("Antoine Griezmann", 4, 3.0), ("Kylian MBappe", 4, 2.5), ("Paul Pogba", 1, 1.0),
      ("Romelu Lukaku", 4, 2.6666666666666665), ("Eden Hazard", 3, 2.0), ("Kevin De Bruyne", 1, 1.0),
      ("Harry Kane", 6, 3.0), ("John Stones", 2, 1.5), ("Dele Alli", 1, 1.0),
      ("Cristiano Ronaldo", 4, 2.0), ("Pepe", 1, 1.0), ("Ricardo Quaresma", 1, 1.0)
      )
  }

}

case class Player(name: String, team: String, goals: Int, assists: Int)
